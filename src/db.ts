/**
 * pi-libp2p-mesh / db.ts
 *
 * SQLite persistence layer using better-sqlite3 in WAL mode.
 *
 * Stores peer state, broadcast history, and message logs so that context
 * survives agent restarts. WAL mode allows multiple processes/threads to
 * read concurrently while writes remain serialized — ideal for shared-memory
 * access patterns where several pi agent peers read the same DB.
 *
 * Tables:
 *   peers       — all known mesh peers (connected, disconnected, stale)
 *   broadcasts  — capped broadcast history (last N entries)
 *   messages    — direct message log (incoming + outgoing)
 *   kv          — simple key-value store for config / session metadata
 */

import Database from "better-sqlite3";
import path from "node:path";
import os from "node:os";
import fs from "node:fs";

import type {
  MeshPeer,
  MeshMemory,
  BroadcastMessage,
} from "./types.js";

// ── Constants ────────────────────────────────────────────────────────────────

/** Maximum broadcast history entries to retain in the DB. */
export const MAX_BROADCAST_HISTORY = 200;

/** Default database file path (inside the project's .pi data dir if available). */
export const DEFAULT_DB_PATH = path.join(
  process.env.PI_DATA_DIR ?? path.join(os.homedir(), ".pi"),
  "mesh.db",
);

// ── Schema ───────────────────────────────────────────────────────────────────

const SCHEMA = /* sql */ `
-- Peers: every P2P node we've ever seen on the mesh
CREATE TABLE IF NOT EXISTS peers (
  id                TEXT PRIMARY KEY,       -- base58 PeerId
  addresses         TEXT NOT NULL DEFAULT '[]',  -- JSON array of multiaddrs
  status            TEXT NOT NULL DEFAULT 'disconnected',  -- connected | disconnected
  agent_name        TEXT,                   -- human-readable name (from Identify)
  discovered_at     INTEGER NOT NULL,       -- epoch ms
  disconnected_at   INTEGER,                -- epoch ms (NULL if connected)
  session_id        TEXT NOT NULL DEFAULT '' -- session that discovered this peer
);

CREATE INDEX IF NOT EXISTS idx_peers_status ON peers(status);
CREATE INDEX IF NOT EXISTS idx_peers_agent_name ON peers(agent_name);
CREATE INDEX IF NOT EXISTS idx_peers_disconnected_at ON peers(disconnected_at);

-- Broadcasts: capped log of GossipSub broadcasts
CREATE TABLE IF NOT EXISTS broadcasts (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  from_agent  TEXT NOT NULL,
  from_peer_id TEXT NOT NULL,
  timestamp   INTEGER NOT NULL,       -- epoch ms
  message     TEXT NOT NULL,
  type        TEXT,                   -- announce | query | response | event
  session_id  TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_broadcasts_timestamp ON broadcasts(timestamp);

-- Messages: log of direct agent-to-agent messages (incoming + outgoing)
CREATE TABLE IF NOT EXISTS messages (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  direction   TEXT NOT NULL,          -- 'incoming' | 'outgoing'
  peer_id     TEXT NOT NULL,          -- remote PeerId
  request_id  TEXT,                   -- UUID of the request
  from_agent  TEXT NOT NULL,
  message     TEXT NOT NULL,
  response    TEXT,                   -- response text (for outgoing after reply arrives)
  error       INTEGER NOT NULL DEFAULT 0,
  timestamp   INTEGER NOT NULL,
  session_id  TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_messages_timestamp ON messages(timestamp);
CREATE INDEX IF NOT EXISTS idx_messages_peer_id ON messages(peer_id);

-- Memories: structured, queryable agent memory about peers and topics
CREATE TABLE IF NOT EXISTS memories (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  peer_id     TEXT,                       -- which peer this is about (NULL for general)
  agent_name  TEXT,                       -- which agent this is about
  key         TEXT NOT NULL,              -- semantic key/topic
  value       TEXT NOT NULL,              -- memory content
  tags        TEXT NOT NULL DEFAULT '[]', -- JSON array of tag strings
  importance  INTEGER NOT NULL DEFAULT 1, -- 1-5 scale
  created_at  INTEGER NOT NULL,
  updated_at  INTEGER NOT NULL,
  session_id  TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_memories_peer_id ON memories(peer_id);
CREATE INDEX IF NOT EXISTS idx_memories_key ON memories(key);
CREATE INDEX IF NOT EXISTS idx_memories_agent_name ON memories(agent_name);
CREATE INDEX IF NOT EXISTS idx_memories_importance ON memories(importance);

-- KV store for config values and runtime metadata
CREATE TABLE IF NOT EXISTS kv (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
`;

// ── Database Class ───────────────────────────────────────────────────────────

/**
 * Manages the SQLite database for the pi-libp2p-mesh extension.
 *
 * - Opens a single connection (better-sqlite3 is synchronous; one connection
 *   is safe per process).
 * - Applies WAL mode for concurrent read support.
 * - Provides typed CRUD helpers for peers, broadcasts, messages, and kv.
 *
 * @example
 * ```ts
 * const db = new MeshDatabase("/path/to/mesh.db");
 * db.upsertPeer({ id: "12D3KooW…", status: "connected", ... });
 * const peers = db.getAllPeers();
 * db.close();
 * ```
 */
export class MeshDatabase {
  private db: Database.Database;

  // ── Prepared statements (created once, reused) ─────────────────────────

  // Peers
  private stmtUpsertPeer!: Database.Statement;
  private stmtGetPeer!: Database.Statement;
  private stmtGetAllPeers!: Database.Statement;
  private stmtGetConnectedPeers!: Database.Statement;
  private stmtPruneStale!: Database.Statement;
  private stmtPruneAllDisconnected!: Database.Statement;
  private stmtPruneDedupByName!: Database.Statement;
  private stmtDisconnectOtherSessions!: Database.Statement;

  // Broadcasts
  private stmtInsertBroadcast!: Database.Statement;
  private stmtGetBroadcasts!: Database.Statement;
  private stmtCountBroadcasts!: Database.Statement;
  private stmtDeleteOldestBroadcasts!: Database.Statement;

  // Messages
  private stmtInsertMessage!: Database.Statement;
  private stmtGetMessages!: Database.Statement;

  // Memories
  private stmtStoreMemory!: Database.Statement;
  private stmtGetMemory!: Database.Statement;
  private stmtRecallByPeer!: Database.Statement;
  private stmtRecallByKey!: Database.Statement;
  private stmtRecallByAgent!: Database.Statement;
  private stmtSearchMemories!: Database.Statement;
  private stmtForgetMemory!: Database.Statement;
  private stmtForgetByKey!: Database.Statement;
  private stmtUpdateMemory!: Database.Statement;
  private stmtGetAllMemories!: Database.Statement;
  private stmtGetMemoriesCount!: Database.Statement;

  // KV
  private stmtGetKv!: Database.Statement;
  private stmtSetKv!: Database.Statement;

  /**
   * Open (or create) the SQLite database at the given path.
   *
   * @param dbPath - Path to the SQLite file. Defaults to `~/.pi/mesh.db`.
   * @param sessionId - Optional identifier for the current session for filtering.
   */
  constructor(
    dbPath: string = DEFAULT_DB_PATH,
    private sessionId: string = "",
  ) {
    // Ensure the parent directory exists
    const dir = path.dirname(dbPath);
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
    }

    this.db = new Database(dbPath);

    // Apply WAL + pragmas
    this.db.pragma("journal_mode = WAL");
    this.db.pragma("synchronous = NORMAL");
    this.db.pragma("busy_timeout = 5000");
    this.db.pragma("foreign_keys = ON");
    this.db.pragma("cache_size = -4000");
    this.db.pragma("mmap_size = 536870912");

    // Create schema
    this.db.exec(SCHEMA);

    // Prepare statements
    this.prepareStatements();
  }

  // ── Peers ─────────────────────────────────────────────────────────────────

  /**
   * Insert or update a peer in the database (ON CONFLICT upsert).
   * Returns the resulting {@link MeshPeer}.
   */
  upsertPeer(peer: MeshPeer): MeshPeer {
    const addresses = JSON.stringify(peer.addresses);
    this.stmtUpsertPeer.run(
      peer.id,
      addresses,
      peer.status,
      peer.agentName ?? null,
      peer.discoveredAt,
      peer.disconnectedAt ?? null,
      this.sessionId,
    );

    // Ensure agentName is not undefined (null coalesce to undefined)
    return {
      ...peer,
      agentName: peer.agentName ?? undefined,
    };
  }

  /**
   * Get a single peer by PeerId string.
   */
  getPeer(id: string): MeshPeer | undefined {
    const row = this.stmtGetPeer.get(id) as RowPeer | undefined;
    return row ? rowToPeer(row) : undefined;
  }

  /**
   * Get all known peers from the database.
   */
  getAllPeers(): MeshPeer[] {
    const rows = this.stmtGetAllPeers.all() as RowPeer[];
    return rows.map(rowToPeer);
  }

  /**
   * Get only connected peers.
   */
  getConnectedPeers(): MeshPeer[] {
    const rows = this.stmtGetConnectedPeers.all() as RowPeer[];
    return rows.map(rowToPeer);
  }

  /**
   * Prune stale peers: remove those disconnected for longer than `ttlMs`.
   * Returns the number of removed entries.
   */
  pruneStale(ttlMs: number = 60_000): number {
    const cutoff = Date.now() - ttlMs;
    const info = this.stmtPruneStale.run(cutoff);
    return info.changes;
  }

  /**
   * Remove all disconnected peers immediately.
   * Returns the number of removed entries.
   */
  pruneAllDisconnected(): number {
    const info = this.stmtPruneAllDisconnected.run();
    return info.changes;
  }

  /**
   * Dedup peers by agent name: when two entries share the same name, keep
   * the connected one and remove the disconnected duplicates.
   * Returns the number of removed entries.
   */
  pruneDedupByName(): number {
    const info = this.stmtPruneDedupByName.run();
    return info.changes;
  }

  /**
   * Mark all connected peers from previous sessions as disconnected.
   * Called on session_start so stale "connected" entries from old sessions
   * don't persist across restarts.
   * Returns the number of peers marked disconnected.
   */
  disconnectPeersFromOtherSessions(): number {
    const info = this.stmtDisconnectOtherSessions.run(Date.now(), this.sessionId);
    return info.changes;
  }

  // ── Broadcasts ────────────────────────────────────────────────────────────

  /**
   * Record a broadcast message, evicting the oldest entry if the cap is exceeded.
   */
  recordBroadcast(msg: BroadcastMessage): void {
    this.stmtInsertBroadcast.run(
      msg.fromAgent,
      msg.fromPeerId,
      msg.timestamp,
      msg.message,
      msg.type ?? null,
      this.sessionId,
    );

    // Evict oldest if over cap
    const count = this.stmtCountBroadcasts.get() as { cnt: number };
    if (count.cnt > MAX_BROADCAST_HISTORY) {
      const excess = count.cnt - MAX_BROADCAST_HISTORY;
      this.stmtDeleteOldestBroadcasts.run(excess);
    }
  }

  /**
   * Get the broadcast history, newest first.
   */
  getBroadcasts(limit: number = MAX_BROADCAST_HISTORY): BroadcastMessage[] {
    const rows = this.stmtGetBroadcasts.all(limit) as RowBroadcast[];
    return rows.map(rowToBroadcast);
  }

  // ── Messages ──────────────────────────────────────────────────────────────

  /**
   * Log a direct message (incoming or outgoing, with optional response).
   */
  logMessage(params: {
    direction: "incoming" | "outgoing";
    peerId: string;
    requestId?: string;
    fromAgent: string;
    message: string;
    response?: string;
    error?: boolean;
  }): void {
    this.stmtInsertMessage.run(
      params.direction,
      params.peerId,
      params.requestId ?? null,
      params.fromAgent,
      params.message,
      params.response ?? null,
      params.error ? 1 : 0,
      Date.now(),
      this.sessionId,
    );
  }

  /**
   * Get message log entries, newest first.
   */
  getMessages(limit: number = 100): RowMessage[] {
    return this.stmtGetMessages.all(limit) as RowMessage[];
  }

  // ── Memories ─────────────────────────────────────────────────────────────

  /**
   * Store a new memory, or update an existing one by (peer_id, agent_name, key).
   * Returns the stored {@link MeshMemory} with the assigned ID.
   */
  storeMemory(memory: Omit<MeshMemory, "id" | "createdAt" | "updatedAt"> & { id?: number }): MeshMemory {
    const now = Date.now();
    const tags = JSON.stringify(memory.tags);

    if (memory.id != null) {
      // Update existing memory by ID
      this.stmtUpdateMemory.run(
        memory.key,
        memory.value,
        tags,
        memory.importance,
        now,
        memory.id,
      );
      return { ...memory, id: memory.id, createdAt: now, updatedAt: now } as MeshMemory;
    }

    const info = this.stmtStoreMemory.run(
      memory.peerId ?? null,
      memory.agentName ?? null,
      memory.key,
      memory.value,
      tags,
      memory.importance,
      now,
      now,
      this.sessionId,
    );

    return {
      id: Number(info.lastInsertRowid),
      peerId: memory.peerId,
      agentName: memory.agentName,
      key: memory.key,
      value: memory.value,
      tags: memory.tags,
      importance: memory.importance,
      createdAt: now,
      updatedAt: now,
    };
  }

  /**
   * Retrieve a single memory by its numeric ID.
   */
  getMemory(id: number): MeshMemory | undefined {
    const row = this.stmtGetMemory.get(id) as RowMemory | undefined;
    return row ? rowToMemory(row) : undefined;
  }

  /**
   * Recall all memories about a specific peer (by PeerId).
   */
  recallByPeer(peerId: string): MeshMemory[] {
    const rows = this.stmtRecallByPeer.all(peerId) as RowMemory[];
    return rows.map(rowToMemory);
  }

  /**
   * Recall memories by semantic key/topic (exact match).
   */
  recallByKey(key: string): MeshMemory[] {
    const rows = this.stmtRecallByKey.all(key) as RowMemory[];
    return rows.map(rowToMemory);
  }

  /**
   * Recall memories about a specific agent (by name).
   */
  recallByAgent(agentName: string): MeshMemory[] {
    const rows = this.stmtRecallByAgent.all(agentName) as RowMemory[];
    return rows.map(rowToMemory);
  }

  /**
   * Full-text search across memory key and value fields (LIKE-based).
   */
  searchMemories(query: string, limit: number = 20): MeshMemory[] {
    const pattern = `%${query}%`;
    const rows = this.stmtSearchMemories.all(pattern, pattern, limit) as RowMemory[];
    return rows.map(rowToMemory);
  }

  /**
   * Delete a memory by its numeric ID.
   * Returns true if a row was deleted.
   */
  forgetMemory(id: number): boolean {
    const info = this.stmtForgetMemory.run(id);
    return info.changes > 0;
  }

  /**
   * Delete all memories matching a given key.
   * Returns the number of deleted rows.
   */
  forgetByKey(key: string): number {
    const info = this.stmtForgetByKey.run(key);
    return info.changes;
  }

  /**
   * Get all memories, most recently updated first.
   */
  getAllMemories(limit: number = 100): MeshMemory[] {
    const rows = this.stmtGetAllMemories.all(limit) as RowMemory[];
    return rows.map(rowToMemory);
  }

  /**
   * Get total memory count.
   */
  getMemoriesCount(): number {
    const row = this.stmtGetMemoriesCount.get() as { cnt: number };
    return row.cnt;
  }

  // ── KV Store ──────────────────────────────────────────────────────────────

  getKV(key: string): string | null {
    const row = this.stmtGetKv.get(key) as { value: string } | undefined;
    return row?.value ?? null;
  }

  setKV(key: string, value: string): void {
    this.stmtSetKv.run(key, value);
  }

  // ── Lifecycle ─────────────────────────────────────────────────────────────

  /**
   * Close the database connection gracefully.
   *
   * better-sqlite3 databases should be closed when no longer needed.
   */
  close(): void {
    try {
      this.db.close();
    } catch {
      // Already closed — ignore.
    }
  }

  /**
   * Run a WAL checkpoint to flush outstanding WAL records to the main DB.
   * Call sparingly (e.g. on shutdown) since WAL auto-checkpoints.
   */
  checkpoint(): void {
    try {
      this.db.pragma("wal_checkpoint(TRUNCATE)");
    } catch {
      // SQLITE_BUSY — another process is reading the WAL. Data is safe;
      // WAL will auto-checkpoint on next write. Log for visibility.
      console.warn("[MeshDatabase] wal_checkpoint(TRUNCATE) failed — WAL file may persist until next write");
    }
  }

  // ── Private ──────────────────────────────────────────────────────────────

  private prepareStatements(): void {
    // Peers
    this.stmtUpsertPeer = this.db.prepare(/* sql */ `
      INSERT INTO peers (id, addresses, status, agent_name, discovered_at, disconnected_at, session_id)
      VALUES (?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(id) DO UPDATE SET
        addresses = excluded.addresses,
        status = excluded.status,
        agent_name = COALESCE(excluded.agent_name, peers.agent_name),
        discovered_at = MIN(peers.discovered_at, excluded.discovered_at),
        disconnected_at = excluded.disconnected_at,
        session_id = excluded.session_id
    `);

    this.stmtGetPeer = this.db.prepare(/* sql */ `
      SELECT * FROM peers WHERE id = ?
    `);

    this.stmtGetAllPeers = this.db.prepare(/* sql */ `
      SELECT * FROM peers ORDER BY discovered_at DESC
    `);

    this.stmtGetConnectedPeers = this.db.prepare(/* sql */ `
      SELECT * FROM peers WHERE status = 'connected' ORDER BY discovered_at DESC
    `);

    this.stmtPruneStale = this.db.prepare(/* sql */ `
      DELETE FROM peers
      WHERE status = 'disconnected'
        AND COALESCE(disconnected_at, discovered_at) < ?
    `);

    this.stmtPruneAllDisconnected = this.db.prepare(/* sql */ `
      DELETE FROM peers WHERE status = 'disconnected'
    `);

    // Dedup: delete disconnected peers whose agent_name matches a connected peer
    this.stmtPruneDedupByName = this.db.prepare(/* sql */ `
      DELETE FROM peers
      WHERE status = 'disconnected'
        AND agent_name IS NOT NULL
        AND agent_name IN (
          SELECT agent_name FROM peers WHERE status = 'connected' AND agent_name IS NOT NULL
        )
    `);

    // Disconnect peers from other sessions (mark stale connected peers as disconnected)
    this.stmtDisconnectOtherSessions = this.db.prepare(/* sql */ `
      UPDATE peers
      SET status = 'disconnected', disconnected_at = ?
      WHERE status = 'connected' AND session_id != ?
    `);

    // Broadcasts
    this.stmtInsertBroadcast = this.db.prepare(/* sql */ `
      INSERT INTO broadcasts (from_agent, from_peer_id, timestamp, message, type, session_id)
      VALUES (?, ?, ?, ?, ?, ?)
    `);

    this.stmtGetBroadcasts = this.db.prepare(/* sql */ `
      SELECT * FROM broadcasts ORDER BY timestamp DESC LIMIT ?
    `);

    this.stmtCountBroadcasts = this.db.prepare(/* sql */ `
      SELECT COUNT(*) AS cnt FROM broadcasts
    `);

    this.stmtDeleteOldestBroadcasts = this.db.prepare(/* sql */ `
      DELETE FROM broadcasts WHERE id IN (
        SELECT id FROM broadcasts ORDER BY timestamp ASC LIMIT ?
      )
    `);

    // Messages
    this.stmtInsertMessage = this.db.prepare(/* sql */ `
      INSERT INTO messages (direction, peer_id, request_id, from_agent, message, response, error, timestamp, session_id)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);

    this.stmtGetMessages = this.db.prepare(/* sql */ `
      SELECT * FROM messages ORDER BY timestamp DESC LIMIT ?
    `);

    // Memories
    this.stmtStoreMemory = this.db.prepare(/* sql */ `
      INSERT INTO memories (peer_id, agent_name, key, value, tags, importance, created_at, updated_at, session_id)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);

    this.stmtUpdateMemory = this.db.prepare(/* sql */ `
      UPDATE memories
      SET key = ?, value = ?, tags = ?, importance = ?, updated_at = ?
      WHERE id = ?
    `);

    this.stmtGetMemory = this.db.prepare(/* sql */ `
      SELECT * FROM memories WHERE id = ?
    `);

    this.stmtRecallByPeer = this.db.prepare(/* sql */ `
      SELECT * FROM memories
      WHERE peer_id = ?
      ORDER BY importance DESC, updated_at DESC
    `);

    this.stmtRecallByKey = this.db.prepare(/* sql */ `
      SELECT * FROM memories
      WHERE key = ?
      ORDER BY importance DESC, updated_at DESC
    `);

    this.stmtRecallByAgent = this.db.prepare(/* sql */ `
      SELECT * FROM memories
      WHERE agent_name = ?
      ORDER BY importance DESC, updated_at DESC
    `);

    this.stmtSearchMemories = this.db.prepare(/* sql */ `
      SELECT * FROM memories
      WHERE key LIKE ? OR value LIKE ?
      ORDER BY importance DESC, updated_at DESC
      LIMIT ?
    `);

    this.stmtForgetMemory = this.db.prepare(/* sql */ `
      DELETE FROM memories WHERE id = ?
    `);

    this.stmtForgetByKey = this.db.prepare(/* sql */ `
      DELETE FROM memories WHERE key = ?
    `);

    this.stmtGetAllMemories = this.db.prepare(/* sql */ `
      SELECT * FROM memories ORDER BY updated_at DESC LIMIT ?
    `);

    this.stmtGetMemoriesCount = this.db.prepare(/* sql */ `
      SELECT COUNT(*) AS cnt FROM memories
    `);

    // KV
    this.stmtGetKv = this.db.prepare(/* sql */ `
      SELECT value FROM kv WHERE key = ?
    `);

    this.stmtSetKv = this.db.prepare(/* sql */ `
      INSERT INTO kv (key, value) VALUES (?, ?)
      ON CONFLICT(key) DO UPDATE SET value = excluded.value
    `);
  }
}

// ── Row Types (internal) ─────────────────────────────────────────────────────

interface RowPeer {
  id: string;
  addresses: string;
  status: string;
  agent_name: string | null;
  discovered_at: number;
  disconnected_at: number | null;
  session_id: string;
}

function rowToPeer(row: RowPeer): MeshPeer {
  // Runtime guard: catches schema drift (renamed/dropped column) early
  if (typeof row.id !== "string") throw new Error("MeshDatabase: peers.id is not a string — schema drift?");
  if (typeof row.status !== "string") throw new Error("MeshDatabase: peers.status is not a string — schema drift?");
  if (typeof row.discovered_at !== "number") throw new Error("MeshDatabase: peers.discovered_at is not a number — schema drift?");

  return {
    id: row.id,
    addresses: safeJsonParse<string[]>(row.addresses, []),
    status: row.status as "connected" | "disconnected",
    agentName: row.agent_name ?? undefined,
    discoveredAt: row.discovered_at,
    disconnectedAt: row.disconnected_at ?? undefined,
  };
}

interface RowBroadcast {
  from_agent: string;
  from_peer_id: string;
  timestamp: number;
  message: string;
  type: string | null;
}

function rowToBroadcast(row: RowBroadcast): BroadcastMessage {
  if (typeof row.from_agent !== "string") throw new Error("MeshDatabase: broadcasts.from_agent is not a string — schema drift?");
  if (typeof row.timestamp !== "number") throw new Error("MeshDatabase: broadcasts.timestamp is not a number — schema drift?");

  return {
    fromAgent: row.from_agent,
    fromPeerId: row.from_peer_id,
    timestamp: row.timestamp,
    message: row.message,
    type: (row.type as BroadcastMessage["type"]) ?? undefined,
  };
}

interface RowMemory {
  id: number;
  peer_id: string | null;
  agent_name: string | null;
  key: string;
  value: string;
  tags: string;
  importance: number;
  created_at: number;
  updated_at: number;
  session_id: string;
}

function rowToMemory(row: RowMemory): MeshMemory {
  if (typeof row.id !== "number") throw new Error("MeshDatabase: memories.id is not a number — schema drift?");
  if (typeof row.key !== "string") throw new Error("MeshDatabase: memories.key is not a string — schema drift?");
  if (typeof row.importance !== "number") throw new Error("MeshDatabase: memories.importance is not a number — schema drift?");

  return {
    id: row.id,
    peerId: row.peer_id ?? undefined,
    agentName: row.agent_name ?? undefined,
    key: row.key,
    value: row.value,
    tags: safeJsonParse<string[]>(row.tags, []),
    importance: row.importance,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}

export interface RowMessage {
  id: number;
  direction: "incoming" | "outgoing";
  peer_id: string;
  request_id: string | null;
  from_agent: string;
  message: string;
  response: string | null;
  error: number;
  timestamp: number;
  session_id: string;
}

// ── Helpers ─────────────────────────────────────────────────────────────────

function safeJsonParse<T>(raw: string, fallback: T): T {
  try {
    return JSON.parse(raw) as T;
  } catch {
    return fallback;
  }
}

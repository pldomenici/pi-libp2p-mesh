/**
 * pi-libp2p-mesh / memory.ts
 *
 * ChromaDB-backed persistent agent memory. Stores conversation exchanges,
 * broadcasts, and explicit key-value memories as vector-embedded documents.
 * Supports semantic search via the all-MiniLM-L6-v2 embedding model (local WASM).
 *
 * Design: append-only log — every store() creates a new entry with a unique ID.
 * The same (peerId, key) pair can accumulate many entries over a run.
 */

import { ChromaClient } from "chromadb";
import { DefaultEmbeddingFunction } from "@chroma-core/default-embed";
import type { Collection } from "chromadb";
import { v4 as uuidv4 } from "uuid";

import type {
  MemoryConfig,
  MemoryEntry,
  MemorySearchResult,
  MemoryKeyEntry,
} from "./types.js";
import { MEMORY_PRESETS } from "./types.js";

// ── AgentMemory ──────────────────────────────────────────────────────────────

export class AgentMemory {
  readonly agentName: string;
  readonly collectionName: string;
  readonly config: MemoryConfig;

  private collection: Collection;
  private client: ChromaClient;
  private embedder: DefaultEmbeddingFunction;

  private constructor(
    client: ChromaClient,
    embedder: DefaultEmbeddingFunction,
    collection: Collection,
    collectionName: string,
    agentName: string,
    config: MemoryConfig,
  ) {
    this.client = client;
    this.embedder = embedder;
    this.collection = collection;
    this.collectionName = collectionName;
    this.agentName = agentName;
    this.config = config;
  }

  // ── Factory ────────────────────────────────────────────────────────────────

  /** Number of retry attempts for ChromaDB operations. */
  private static readonly CONNECT_RETRIES = 3;
  /** Initial backoff delay in ms (doubles each retry). */
  private static readonly CONNECT_RETRY_BASE_MS = 500;

  static async create(opts: {
    host?: string;
    port?: number;
    token?: string;
    agentName: string;
    config?: MemoryConfig;
  }): Promise<AgentMemory> {
    const host = opts.host ?? "localhost";
    const port = opts.port ?? 8000;
    const agentName = opts.agentName;
    const config = opts.config ?? MEMORY_PRESETS.large;

    // Initialize the default embedding function (all-MiniLM-L6-v2, local WASM).
    // This is synchronous — the model downloads lazily on first embed() call.
    let embedder: DefaultEmbeddingFunction;
    try {
      embedder = new DefaultEmbeddingFunction();
    } catch (err: any) {
      throw new Error(`Embedding function initialization failed: ${err.message}`);
    }

    // Connect to ChromaDB — pass auth token via x-chroma-token header if provided
    const headers: Record<string, string> = {};
    if (opts.token) headers["x-chroma-token"] = opts.token;
    const client = new ChromaClient({ host, port, ssl: false, ...(opts.token ? { headers } : {}) });

    // Verify ChromaDB is reachable with a heartbeat check before creating the collection
    try {
      await client.heartbeat();
    } catch (err: any) {
      throw new Error(`ChromaDB unreachable at ${host}:${port}: ${err.message}`);
    }

    // Get or create the shared mesh memory collection.
    // All agents share one collection so knowledge is accessible across the mesh.
    const collectionName = "pi_memory_mesh";
    let lastErr: Error | undefined;

    for (let attempt = 1; attempt <= AgentMemory.CONNECT_RETRIES; attempt++) {
      try {
        const collection = await client.getOrCreateCollection({
          name: collectionName,
          embeddingFunction: embedder,
        });

        console.log(
          `[pi-libp2p-mesh] AgentMemory connected to ChromaDB at ${host}:${port}, collection "${collectionName}"`,
        );

        return new AgentMemory(client, embedder, collection, collectionName, agentName, config);
      } catch (err: any) {
        lastErr = err;
        if (attempt < AgentMemory.CONNECT_RETRIES) {
          const delay = AgentMemory.CONNECT_RETRY_BASE_MS * Math.pow(2, attempt - 1);
          console.warn(
            `[pi-libp2p-mesh] Collection creation attempt ${attempt}/${AgentMemory.CONNECT_RETRIES} failed: ${err.message} — retrying in ${delay}ms`,
          );
          await new Promise((r) => setTimeout(r, delay));
        }
      }
    }

    throw new Error(
      `Collection "${collectionName}" creation failed after ${AgentMemory.CONNECT_RETRIES} attempts: ${lastErr?.message}`,
    );
  }

  // ── Resilience ─────────────────────────────────────────────────────────────

  /**
   * Wrap a ChromaDB operation with automatic collection-refresh on "not found"
   * errors. If the collection was deleted and recreated (or the server restarted),
   * the first failure triggers a single reconnection attempt, then the operation
   * is retried once.
   */
  private async _withRetry<T>(fn: (coll: Collection) => Promise<T>): Promise<T> {
    try {
      return await fn(this.collection);
    } catch (err: any) {
      if (this._isNotFound(err)) {
        console.warn(
          `[pi-libp2p-mesh] Collection "${this.collectionName}" not found — reconnecting…`,
        );
        this.collection = await this.client.getOrCreateCollection({
          name: this.collectionName,
          embeddingFunction: this.embedder,
        });
        console.warn(
          `[pi-libp2p-mesh] Reconnected to collection "${this.collectionName}"`,
        );
        return await fn(this.collection);
      }
      throw err;
    }
  }

  /** Detect ChromaDB "not found" / 404 errors from the HTTP response. */
  private _isNotFound(err: any): boolean {
    // ChromaNotFoundError class — the most reliable signal
    if (err?.constructor?.name === 'ChromaNotFoundError') return true;

    const msg: string = err?.message ?? '';
    // ChromaDB messages: "could not be found", "does not exist", "not found"
    if (msg.includes('not be found') || msg.includes('not found') || msg.includes('does not exist')) return true;

    if (err?.status === 404 || err?.code === 404) return true;

    // The ChromaDB JS client may wrap fetch errors — check the cause chain
    if (err?.cause) return this._isNotFound(err.cause);
    return false;
  }

  // ── Store ──────────────────────────────────────────────────────────────────

  /**
   * Store a memory entry. Append-only — every call creates a new entry
   * with a unique ID. The same (peerId, key) pair can accumulate many
   * entries over a run.
   */
  async store(entry: {
    peerId: string;
    key: string;
    value: string;
    metadata?: Record<string, string | number>;
  }): Promise<void> {
    const id = `${entry.peerId}:${entry.key}:${Date.now()}:${uuidv4()}`;

    await this._withRetry((coll) =>
      coll.add({
        ids: [id],
        documents: [entry.value],
        metadatas: [
          {
            peerId: entry.peerId,
            agentName: this.agentName,
            key: entry.key,
            timestamp: Date.now(),
            type: entry.metadata?.type ?? "explicit",
            ...entry.metadata,
          },
        ],
      }),
    );
  }

  // ── Retrieve ───────────────────────────────────────────────────────────────

  /**
   * Get memory entries for (peerId, key), newest first.
   * At least one of peerId or key must be provided.
   * Values are truncated unless fullText=true. Hard max enforced.
   */
  async get(
    peerId: string | undefined,
    key: string | undefined,
    opts?: { limit?: number; fullText?: boolean },
  ): Promise<MemoryEntry[]> {
    if (!peerId && !key) {
      throw new Error("At least one of peerId or key is required");
    }

    const limit = Math.min(opts?.limit ?? this.config.defaultLimit, this.config.maxEntries);

    // Build where clause — ChromaDB requires $and for multiple filters
    let where: Record<string, unknown> | undefined;
    if (peerId && key) {
      where = { $and: [{ peerId }, { key }] };
    } else if (peerId) {
      where = { peerId };
    } else if (key) {
      where = { key };
    }

    const result = await this._withRetry((coll) =>
      coll.get({
        where: where as any,
        limit,
        include: ["documents", "metadatas"],
      }),
    );

    if (!result.ids.length) return [];

    const entries = result.ids.map((id, i) => ({
      id,
      peerId: (result.metadatas[i] as any).peerId ?? "",
      key: (result.metadatas[i] as any).key ?? "",
      value: this._truncate(result.documents[i] ?? "", opts?.fullText),
      timestamp: (result.metadatas[i] as any).timestamp ?? 0,
      type: (result.metadatas[i] as any).type,
      metadata: result.metadatas[i] as Record<string, string | number>,
    }));

    // Sort newest first (ChromaDB returns insertion order, oldest first)
    entries.sort((a, b) => b.timestamp - a.timestamp);

    return entries;
  }

  /**
   * Get all memories for a given peer, across all keys, newest first.
   * Values truncated unless fullText=true. Hard max enforced.
   */
  async getByPeer(
    peerId: string,
    opts?: { limit?: number; fullText?: boolean },
  ): Promise<MemoryEntry[]> {
    return this.get(peerId, undefined, opts);
  }

  // ── Search ─────────────────────────────────────────────────────────────────

  /**
   * Semantic search: embed `query` and find top-N memory documents
   * most similar to it. Values truncated unless fullText=true.
   * Entries with distance > threshold are filtered out.
   * Queries for 2× nResults so distance filtering doesn't short-change results.
   */
  async search(
    query: string,
    opts?: {
      peerId?: string;
      nResults?: number;
      fullText?: boolean;
    },
  ): Promise<MemorySearchResult[]> {
    const n = opts?.nResults ?? this.config.searchNResults;
    const queryN = n * 2;

    const where: Record<string, string> = {};
    if (opts?.peerId) where.peerId = opts.peerId;

    const results = await this._withRetry((coll) =>
      coll.query({
        queryTexts: [query],
        nResults: queryN,
        where: Object.keys(where).length > 0 ? where : undefined,
      }),
    );

    if (!results.ids[0]?.length) return [];

    const mapped: MemorySearchResult[] = [];
    const len = Math.min(
      results.ids[0].length,
      results.distances[0]?.length ?? 0,
      results.metadatas[0]?.length ?? 0,
      results.documents[0]?.length ?? 0,
    );
    for (let i = 0; i < len && mapped.length < n; i++) {
      const distance = results.distances[0][i];
      if (distance == null || distance > this.config.distanceThreshold) continue;

      mapped.push({
        id: results.ids[0][i],
        peerId: (results.metadatas[0][i] as any).peerId ?? "",
        key: (results.metadatas[0][i] as any).key ?? "",
        value: this._truncate(results.documents[0][i] ?? "", opts?.fullText),
        timestamp: (results.metadatas[0][i] as any).timestamp ?? 0,
        type: (results.metadatas[0][i] as any).type,
        metadata: results.metadatas[0][i] as Record<string, string | number>,
        distance,
      });
    }

    return mapped;
  }

  // ── Keys ────────────────────────────────────────────────────────────────────

  /**
   * List all unique keys stored for a peer, with entry counts.
   */
  async getKeys(peerId: string): Promise<MemoryKeyEntry[]> {
    // Use a generous limit to avoid loading excessive metadata into memory
    const KEY_SCAN_LIMIT = 2000;
    const result = await this._withRetry((coll) =>
      coll.get({
        where: { peerId } as any,
        limit: KEY_SCAN_LIMIT,
        include: ["metadatas"],
      }),
    );

    const byKey = new Map<string, number>();
    for (const meta of result.metadatas) {
      const key = (meta as any).key as string;
      byKey.set(key, (byKey.get(key) ?? 0) + 1);
    }

    return [...byKey.entries()]
      .map(([key, count]) => ({ key, count }))
      .sort((a, b) => b.count - a.count);
  }

  // ── Count ──────────────────────────────────────────────────────────────────

  async count(peerId?: string): Promise<number> {
    // Use a large limit for safety — practical per-peer entry counts are < 10K
    const COUNT_LIMIT = 100_000;
    if (peerId) {
      const result = await this._withRetry((coll) =>
        coll.get({
          where: { peerId } as any,
          limit: COUNT_LIMIT,
          include: [],
        }),
      );
      return result.ids.length;
    }

    const result = await this._withRetry((coll) =>
      coll.get({
        limit: COUNT_LIMIT,
        include: [],
      }),
    );
    return result.ids.length;
  }

  // ── Delete ─────────────────────────────────────────────────────────────────

  /** Maximum entries to fetch per batch in deleteByPeer. */
  private static readonly DELETE_BATCH_SIZE = 500;

  async deleteByPeer(peerId: string): Promise<number> {
    let totalDeleted = 0;

    // Paginate: fetch and delete in batches to handle large peer histories
    while (true) {
      const existing = await this._withRetry((coll) =>
        coll.get({
          where: { peerId } as any,
          limit: AgentMemory.DELETE_BATCH_SIZE,
          include: [],
        }),
      );

      if (!existing.ids.length) break;

      await this._withRetry((coll) =>
        coll.delete({ ids: existing.ids as any }),
      );
      totalDeleted += existing.ids.length;

      // If we got fewer than the batch size, we've reached the end
      if (existing.ids.length < AgentMemory.DELETE_BATCH_SIZE) break;
    }

    return totalDeleted;
  }

  async deleteById(id: string): Promise<void> {
    await this._withRetry((coll) =>
      coll.delete({ ids: [id] as any }),
    );
  }

  // ── Lifecycle ──────────────────────────────────────────────────────────────

  /**
   * Manually refresh the collection reference. Use after ChromaDB restarts
   * or if the collection was externally deleted and recreated.
   */
  async reconnect(): Promise<void> {
    this.collection = await this.client.getOrCreateCollection({
      name: this.collectionName,
      embeddingFunction: this.embedder,
    });
    console.log(
      `[pi-libp2p-mesh] AgentMemory reconnected to collection "${this.collectionName}"`,
    );
  }

  /** No-op — ChromaDB client is stateless HTTP. */
  async stop(): Promise<void> {
    // Nothing to close
  }

  // ── Private helpers ────────────────────────────────────────────────────────

  private _truncate(value: string, fullText?: boolean): string {
    if (fullText) return value;
    if (value.length <= this.config.valueTruncationChars) return value;
    return value.slice(0, this.config.valueTruncationChars) + "… [truncated]";
  }
}

// ── Config resolution ────────────────────────────────────────────────────────

/**
 * Resolve a MemoryConfig from CLI flags, environment variables, and presets.
 *
 * Priority: CLI flag > environment variable > preset default
 */
export function resolveMemoryConfig(overrides?: {
  preset?: string;
  maxEntries?: number;
  truncate?: number;
  budget?: number;
  exchangeTruncate?: number;
  distance?: number;
}): MemoryConfig {
  // Start with a preset — validate and fallback to "large"
  const presetName = overrides?.preset ?? "large";
  const base = MEMORY_PRESETS[presetName as keyof typeof MEMORY_PRESETS];
  if (!base) {
    console.warn(
      `[pi-libp2p-mesh] Unknown memory preset "${presetName}" — falling back to "large"`,
    );
  }
  const resolved = { ...(base ?? MEMORY_PRESETS.large) };

  // Apply individual overrides
  if (overrides?.maxEntries !== undefined) resolved.maxEntries = overrides.maxEntries;
  if (overrides?.truncate !== undefined) resolved.valueTruncationChars = overrides.truncate;
  if (overrides?.budget !== undefined) resolved.contextBudgetChars = overrides.budget;
  if (overrides?.exchangeTruncate !== undefined) resolved.exchangeTruncationChars = overrides.exchangeTruncate;
  if (overrides?.distance !== undefined) resolved.distanceThreshold = overrides.distance;

  return resolved;
}

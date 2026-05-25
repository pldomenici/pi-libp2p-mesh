# pi-libp2p-mesh — Architecture

> **Last updated:** 2026-05-25  
> **Package:** `pi-libp2p-mesh` v0.3.3  
> **Lines of code:** ~2,500 TypeScript across 7 source files

---

## Table of Contents

1. [Overview](#overview)
2. [High-Level Architecture](#high-level-architecture)
3. [Module Breakdown](#module-breakdown)
   - [index.ts — Extension Entry Point & Lifecycle](#indexts--extension-entry-point--lifecycle)
   - [node.ts — MeshNode (libp2p Node)](#nodets--meshnode-libp2p-node)
   - [protocols.ts — MeshProtocols (Messaging)](#protocolsts--meshprotocols-messaging)
   - [tools.ts — LLM Tools](#toolsts--llm-tools)
   - [memory.ts — AgentMemory (ChromaDB)](#memoryts--agentmemory-chromadb)
   - [chroma-lifecycle.ts — ChromaDB Process Management](#chroma-lifecyclets--chromadb-process-management)
   - [types.ts — Shared Types](#typests--shared-types)
4. [Data Flow](#data-flow)
   - [Direct Message Flow (`mesh_send`)](#direct-message-flow-mesh_send)
   - [Broadcast Flow (`mesh_broadcast`)](#broadcast-flow-mesh_broadcast)
   - [LLM Request Queue (FIFO)](#llm-request-queue-fifo)
   - [Memory Persistence Flow](#memory-persistence-flow)
   - [Memory Retrieval Flow](#memory-retrieval-flow)
5. [Protocol Stack](#protocol-stack)
6. [Lifecycle](#lifecycle)
   - [ChromaDB Host Election](#chromadb-host-election)
7. [State Management](#state-management)
8. [Design Decisions & Fixes](#design-decisions--fixes)
9. [Configuration & CLI Flags](#configuration--cli-flags)
10. [Commands](#commands)
11. [Test Suite](#test-suite)
12. [Extension Integration](#extension-integration)
13. [ChromaDB Memory Layer](#chromadb-memory-layer)
14. [AGENT-MEMORY.md — LLM Usage Guide](#agent-memorymd--llm-usage-guide)
15. [Future Considerations](#future-considerations)

---

## Overview

`pi-libp2p-mesh` is a **pi extension** that creates a peer-to-peer overlay network for pi coding agents. Built on [libp2p](https://libp2p.io/) v3, it enables:

- **Automatic peer discovery** via mDNS (local network) and optional Kademlia DHT (wide-area)
- **Direct agent-to-agent messaging** over a custom libp2p stream protocol (`/pi-agent/0.1.0`)
- **GossipSub broadcast** for group announcements and coordination
- **End-to-end encryption** via the Noise protocol
- **Nine LLM-callable tools** (5 mesh + 4 memory) for the agent to orchestrate P2P workflows and persistent recall
- **Persistent agent memory** via ChromaDB — vector-backed storage of peer interactions, key-value facts, and semantic recall

The extension runs as a singleton within each pi agent process. Every agent that loads this extension becomes a mesh peer with a unique Ed25519-based PeerId.

---

## High-Level Architecture

```
┌─────────────────── Mesh Network ────────────────────┐
│                                                     │
│  ┌── Agent A ─────────────────────────────────────┐ │
│  │  ┌──────────────┐  ┌────────┐  ┌─────────────┐ │ │
│  │  │  index.ts    │◄─┤tools.ts│  │ memory.ts   │ │ │
│  │  │  lifecycle,  │  │ 9 tools│  │ AgentMemory │ │ │
│  │  │  FIFO queue, │  └────────┘  │ ChromaDB    │ │ │
│  │  │  host elect. │              │ client      │ │ │
│  │  └──────┬───────┘              └──────┬──────┘ │ │
│  │         │                            │         │ │
│  │  ┌──────▼────────┐  ┌────────────────┘         │ │
│  │  │ MeshProtocols │  │                          │ │
│  │  │  · sendMessage│  │                          │ │
│  │  │  · broadcast  │  │                          │ │
│  │  │  · raw topic  │  │                          │ │
│  │  └──────┬────────┘  │                          │ │
│  │         │           │                          │ │
│  │  ┌──────▼────────┐  │                          │ │
│  │  │   MeshNode    │  │                          │ │
│  │  │  · libp2p     │  │                          │ │
│  │  └───────────────┘  │                          │ │
│  └─────────────────────┼──────────────────────────┘ │
│                        ▼                            │
│  ┌──────────────── ChromaDB ──────────────────────┐ │
│  │  (one instance, first node hosts)              │ │
│  │  ┌────────────────────────────┐                │ │
│  │  │ pi_memory_*  collection    │                │ │
│  │  │ · all agents share this    │                │ │
│  │  └────────────────────────────┘                │ │
│  └────────────────────────────────────────────────┘ │
│                                                     │
│  ┌── Agent B ─────────────────────────────────────┐ │
│  │  (same structure, connects to  Agent A's       │ │
│  │   ChromaDB via mesh-discovered host)           │ │
│  └────────────────────────────────────────────────┘ │
│                                                     │
│              ┌──────────┴──────────┐                │
│              │   libp2p overlay    │                │
│              │  TCP + WebSocket    │                │
│              │  Noise / Yamux      │                │
│              │  mDNS + DHT         │                │
│              └─────────────────────┘                │
└─────────────────────────────────────────────────────┘
```

### Dependency Graph

```
index.ts
  ├── node.ts ───────── types.ts
  ├── protocols.ts ───── types.ts
  ├── tools.ts ───────── types.ts, protocols.ts (type only), memory.ts (type only)
  ├── memory.ts ──────── types.ts (type only)
  └── chroma-lifecycle.ts ── (standalone — process management)
```

---

## Module Breakdown

### index.ts — Extension Entry Point & Lifecycle

**Path:** `src/index.ts`  
**Role:** The glue layer — wires all modules together and interfaces with the pi `ExtensionAPI`.

**Responsibilities:**

1. **CLI Flag Registration** — registers `--agent-name`, `--mesh-enable-dht`, `--mesh-gossip-topic`, `--mesh-chroma-host`, `--mesh-chroma-port`, `--mesh-memory-preset`, `--mesh-memory-max-entries`, `--mesh-memory-truncate`, `--mesh-memory-budget`, `--mesh-memory-exchange-truncate`, `--mesh-memory-distance`
2. **`session_start` handler** — the boot sequence:
   - Resolves agent name (CLI flag → `PI_MESH_NAME` env var → `PI_COMM_NAME` env var → `pi-<hostname>`)
   - Creates `MeshNode` (libp2p node)
   - Creates `MeshProtocols` (message handler)
   - Sets up the **FIFO LLM request queue** (see [LLM Request Queue](#llm-request-queue-fifo))
   - Starts the node (begin listening, discovery)
   - Runs **ChromaDB host election** — subscribes to `pi-memory-host` topic, starts ChromaDB if first node, connects to existing host otherwise
   - **Initializes `AgentMemory`** — connects to ChromaDB at the resolved host/port, gets/creates `pi_memory_{agentName}` collection
   - Injects memory context into incoming requests (auto-retrieves relevant memories about the requesting peer)
   - Saves memory after LLM responds (auto-saves exchanges)
   - Wires event handlers (peer discovery, connect, disconnect, identify, message, broadcast)
   - Wires inbound broadcasts to `pi.sendUserMessage()` so the LLM sees them
   - Starts background stale-peer pruning (every 30s)
3. **`session_shutdown` handler** — stops the node, cancels pruning timer, tears down protocols, stops ChromaDB child process, closes ChromaDB connection
4. **Command Registration** — `/mesh-auto-reply`, `/mesh-list-peers`, `/mesh-discover`, `/mesh-prune`
5. **Delegates tool registration** to `registerMeshTools()` in `tools.ts` and `registerMemoryTools()` in `tools.ts`

**Singleton state:**

| Variable | Type | Purpose |
|---|---|---|
| `meshNode` | `MeshNode \| null` | Active libp2p node |
| `meshProtocols` | `MeshProtocols \| null` | Active protocol handler |
| `agentMemory` | `AgentMemory \| null` | Active ChromaDB-backed memory store |
| `chromaLifecycle` | `ChromaDBLifecycle \| null` | ChromaDB child process manager |
| `pruneInterval` | `setInterval` handle | Background pruning timer |
| `store` | `MeshStore` | Shared peer registry, broadcast history, agent name, auto-reply flag |

**Memory integration hooks (NEW):**

```
session_start:
  // ── Phase 1: Start mesh (needed for host election) ──
  1. meshNode = await MeshNode.create(config)
  2. meshProtocols = new MeshProtocols(node.libp2p, config)
  3. await meshNode.start()

  // ── Phase 2: ChromaDB host election ──
  4. meshProtocols.subscribeRawTopic("pi-memory-host", ...)
  5. if (localhost:port already running) → announce self as host
     else: wait 2.5s for host announcement via GossipSub
       found → resolvedHost = discovered host
       not found → start ChromaDB, announce self as host

  // ── Phase 3: Connect to ChromaDB ──
  // Resolve memory config: preset → individual overrides → defaults
  // Priority: CLI flag > env var > preset default
  const memoryConfig = resolveMemoryConfig(pi);

  6. agentMemory = await AgentMemory.create({
       host: resolvedHost, port: resolvedPort,
       agentName: store.agentName,
       config: memoryConfig
     })
     → If ChromaDB is unreachable:
        - Log a warning: "ChromaDB unreachable — memory disabled for this session"
        - agentMemory is set to null
        - Mesh operates normally; auto-save/auto-retrieve are no-ops
        - Memory tools return "Memory not available" errors
     → If connected: gets/creates pi_memory_{agentName} collection

turn_end (FIFO queue → after extractResponseText):
  2. if (agentMemory) {
       try {
         await agentMemory.store({...});
       } catch (e) {
         console.debug('[mesh-memory] auto-save failed:', e.message);
         // Silently skip — mesh continues normally
       }
     }

onRequest (meshProtocols.onRequest handler → before enqueue):
  3. if (agentMemory) {
       const memories = await agentMemory.search({...})
       if (memories.length > 0):
         prepend `[Memory: ${peer.agentName}]\n` to the user message
     }

onBroadcast:
  5. await agentMemory.store({
       peerId: msg.fromPeerId,
       key: "broadcast",
       value: `[${msg.type}] ${msg.message}`,
       metadata: { type: "broadcast" }
     })
```

---

### node.ts — MeshNode (libp2p Node)

**Path:** `src/node.ts`  
**Exports:** `MeshNode` class, `EVENT_BUS_TOPIC` constant

**Role:** Factory and lifecycle manager for the libp2p node. Wraps libp2p v3 and emits structured events (`MeshNodeEvent`) upstream.

**Key API:**

```typescript
class MeshNode {
  readonly peerId: string;           // Base58 PeerId
  readonly libp2p: Libp2p;           // Underlying libp2p instance
  multiaddrs: string[];              // Listening addresses (populated after start())

  static create(config: MeshConfig): Promise<MeshNode>;
  start(): Promise<void>;
  stop(): Promise<void>;
  getPeers(): MeshPeer[];
  pruneStalePeers(ttlMs?: number): number;
  pruneAllDisconnected(): number;
  onEvent(handler: MeshNodeEventHandler): void;
}
```

**Factory (`MeshNode.create`):**
1. Generates an Ed25519 keypair (or derives from a persistent seed via `config.privateKey` for stable identity)
2. Assembles the libp2p configuration:
   - **Transports:** TCP + WebSocket
   - **Encryption:** Noise
   - **Muxer:** Yamux
   - **Services:** Identify, GossipSub (topic: `pi-broadcast`)
   - **Discovery:** mDNS (always) + optional Kademlia DHT + Bootstrap
3. Returns a `MeshNode` wrapping the `Libp2p` instance

**Internal peer store (H2 fix):**
- Maintains a `Map<string, MeshPeer>` that persists peers across connect/disconnect cycles
- Disconnected peers stay queryable with `status: "disconnected"` and a `disconnectedAt` timestamp
- Backfills connected peers from libp2p's transient connection list

**Debounced auto-dial (H1 fix):**
- Newly discovered PeerIds are collected in a `Set<string>` (`pendingDials`)
- Dialed in a single batch after a 200ms coalescing window
- Prevents connection storms when multiple peers are discovered simultaneously via mDNS

**Event emissions:**
- `peer:discovered` — a new peer was found (mDNS/DHT)
- `peer:connected` — a transport connection was established
- `peer:disconnected` — a transport connection was dropped
- `peer:identified` — the peer's Identify protocol completed (agent name available)

---

### protocols.ts — MeshProtocols (Messaging)

**Path:** `src/protocols.ts`  
**Exports:** `MeshProtocols` class

**Role:** Implements the messaging layer on top of libp2p streams and GossipSub.

**Constructor:**
1. Registers the `/pi-agent/0.1.0` stream protocol handler
2. Subscribes to GossipSub on `config.gossipTopic` (default: `pi-broadcast`)
3. Listens for `gossipsub:message` events (GossipSub-specific event name, not the generic `message`)

**Public API:**

```typescript
class MeshProtocols {
  sendMessage(peerId: string, request: Omit<AgentRequest, 'fromPeerId' | 'timestamp'>): Promise<AgentResponse>;
  broadcast(message: Omit<BroadcastMessage, 'fromPeerId' | 'timestamp'>): Promise<MeshBroadcastResult>;

  onMessage: (peerId: string, request: AgentRequest) => void;
  onBroadcast: (msg: BroadcastMessage) => void;
  onRequest: (peerId: string, request: AgentRequest) => Promise<string>;

  stop(): Promise<void>;
}
```

**Direct messaging protocol (`/pi-agent/0.1.0`):**

Handshake is a simple CBOR-encoded request-response over a libp2p stream:

1. **Sender:** Opens a stream, writes a CBOR-encoded `AgentRequest`, closes write side
2. **Receiver:** Reads the stream, processes the request, writes a CBOR-encoded `AgentResponse`
3. **Sender:** Reads the response, parses it, returns it to the caller

The protocol includes a **60-second default timeout** (configurable per-request via `timeoutMs`) managed by an `AbortController`. The **tool layer** (`tools.ts` `mesh_send`) implements a retry loop (2 attempts, 500ms delay) for transient dial failures before returning to the LLM — the protocol layer (`protocols.ts` `sendMessage`) does not retry.

**Incoming message handler decision tree:**

```
autoReply === true?
  → YES: echo "[auto-response] Received: ..."
  → NO:  call this._onRequest(peerId, request)
           → this triggers the FIFO queue in index.ts
        if no _onRequest registered:
           → fallback auto-response
```

**GossipSub broadcast:**

- Publishes CBOR-serialized `BroadcastMessage` to the configured topic
- Incoming broadcasts are parsed and forwarded to the `onBroadcast` callback
- `resolvePubsub()` resolves the GossipSub instance from `libp2p.services.pubsub` (v3 pattern) with fallback to `libp2p.pubsub`

**Error handling (M2 fix):**
- If `handleIncomingMessage` encounters an error, it writes an error `AgentResponse` (with `error: true`) back to the sender so they fail fast (<100ms) instead of waiting for the full 60s timeout

**Stream reading:**
- `readStream()` — a helper that reads a libp2p v3 `Stream` (AsyncIterable) into a single `Uint8Array`
- Accepts an `AbortSignal` (H3 fix) to prevent indefinite hangs on unresponsive peers
- Single-pass concatenation — only one copy of the data

---

### tools.ts — LLM Tools

**Path:** `src/tools.ts`  
**Exports:** `registerMeshTools`, `registerMemoryTools`, `setMeshProtocols`, `setAgentMemory`, `listPeers`, `pruneAllDisconnected`, `pruneStalePeers`, `recordBroadcast`, `MeshStore`, `PeerListResult`

**Role:** Registers the LLM-callable tools and manages the shared `MeshStore` state.

#### Mesh Tools (existing — 5 tools)

| Tool | Description | Parameters |
|---|---|---|
| `mesh_list_peers` | List all peers (including self) with status, addresses, agent names | `{}` |
| `mesh_send` | Send a direct message to a peer, await their response | `peerId: string`, `message: string`, `autoReply?: boolean` |
| `mesh_broadcast` | Publish a GossipSub broadcast | `message: string`, `type?: "announce" \| "query" \| "response" \| "event"` |
| `mesh_discover` | Scan for new peers, report recently discovered | `{}` |
| `mesh_prune` | Remove all disconnected/stale peers | `{}` |

#### Memory Tools (NEW — 4 tools)

| Tool | Description | Parameters |
|---|---|---|
| `memory_store` | Save a key-value memory entry associated with a peer | `peerId: string`, `key: string`, `value: string`, `metadata?: object` |
| `memory_recall` | Recall memories for a peer by key (at least one of peerId or key required) | `peerId?: string`, `key?: string`, `limit?: number` |
| `memory_search` | Semantic search across all stored memories | `query: string`, `peerId?: string`, `nResults?: number` |
| `memory_keys` | List all keys stored for a peer, with entry counts | `peerId: string` |

All tools use TypeBox schemas for parameter validation. Tools that require a network call (`mesh_send`, `mesh_broadcast`) provide progress updates via the `onUpdate` callback.

**Shared Store (`MeshStore`):**

```typescript
interface MeshStore {
  peers: Map<string, MeshPeer>;          // Peer registry
  broadcastHistory: BroadcastMessage[];   // Capped at 200 entries
  agentName: string;                     // This agent's name
  autoReplyAll: boolean;                 // Global auto-reply toggle
}
```

**Pruning strategies:**

1. **Agent-name dedup** — if two entries share the same `agentName`, the disconnected one (from a restarted session with a new PeerId) is removed, keeping only the connected entry
2. **Time-based** — peers disconnected for >60 seconds are removed
3. **Aggressive** (`pruneAllDisconnected`) — removes all disconnected peers immediately (used by `/mesh-prune` and `mesh_prune` tool)

**Broadcast history cap (M3 fix):**
- `MAX_BROADCAST_HISTORY = 200` — oldest entries are shifted out when the limit is exceeded

---

### memory.ts — AgentMemory (ChromaDB)

**Path:** `src/memory.ts` (NEW)  
**Exports:** `AgentMemory` class, `MemoryEntry` type, `MemorySearchResult` type

**Role:** ChromaDB-backed persistent memory store for agent context. Each memory entry is a `(peerId, key) → value` mapping stored as a vector embedding document with rich metadata. Enables semantic recall of past interactions, shared facts, and agent-learned knowledge.

**Data model — ChromaDB document structure**

Each memory entry is stored as a ChromaDB document with:

| Field | ChromaDB location | Type | Description |
|---|---|---|---|
| `id` | `ids[]` | `string` | Unique: `{peerId}:{key}:{timestamp}:{uuid4}` |
| `value` (embedding) | `documents[]` | `string` | The full text — this is what gets vector-embedded for semantic search |
| `peerId` | `metadatas[].peerId` | `string` | Peer this memory is associated with |
| `agentName` | `metadatas[].agentName` | `string` | Our own agent name (for per-agent collection scoping) |
| `key` | `metadatas[].key` | `string` | User-defined key (e.g., `"prefs"`, `"project_context"`, `"exchange"`) |
| `timestamp` | `metadatas[].timestamp` | `number` | Epoch ms when stored |
| `type` | `metadatas[].type` | `string` | Category: `"explicit"`, `"conversation_turn"`, `"broadcast"`, `"system"` |

**Collection strategy:**
- One collection per agent: `pi_memory_{agentName}` 
- All memories share the same collection; filtered by metadata `peerId` and `key`
- Rationale: clean isolation per agent instance, no cross-contamination of embedding spaces

**API:**

```typescript
class AgentMemory {
  readonly agentName: string;
  readonly collectionName: string;  // "pi_memory_{agentName}"

  // Factory
  static create(opts: {
    host?: string;
    port?: number;
    agentName: string;
    config?: MemoryConfig;
  }): Promise<AgentMemory>;

  // ── Core Operations ────────────────────────────────────────────────

  /**
   * Store a memory entry. Every call appends a new entry — entries accumulate
   * as a full chronological log of all interactions during the run.
   * The same (peerId, key) pair can have multiple entries over time.
   */
  store(entry: {
    peerId: string;
    key: string;
    value: string;
    metadata?: Record<string, string | number>;
  }): Promise<void>;

  /**
   * Get memory entries for (peerId, key), newest first.
   * Values are truncated to 10,000 chars unless fullText=true.
   * Hard max: 50 entries regardless of limit.
   */
  get(peerId: string, key: string, opts?: {
    limit?: number;
    fullText?: boolean;
  }): Promise<MemoryEntry[]>;

  /**
   * Get all memories for a peer, all keys, newest first.
   * Values truncated to 10,000 chars. Hard max: 50 entries.
   */
  getByPeer(peerId: string, limit?: number): Promise<MemoryEntry[]>;

  /**
   * Semantic search. Values truncated to 10,000 chars.
   * Entries with distance > 0.6 are filtered out.
   */
  search(query: string, opts?: {
    peerId?: string;
    nResults?: number;
    fullText?: boolean;
  }): Promise<MemorySearchResult[]>;

  /**
   * Delete all memories associated with a specific peer.
   */
  deleteByPeer(peerId: string): Promise<number>;

  /**
   * Delete a specific memory entry by its ChromaDB document ID.
   */
  deleteById(id: string): Promise<void>;

  /**
   * Return the count of stored memories, optionally scoped to a peer.
   */
  count(peerId?: string): Promise<number>;

  /**
   * List all unique keys stored for a peer, with entry counts.
   */
  getKeys(peerId: string): Promise<Array<{ key: string; count: number }>>;

  // ── Lifecycle ──────────────────────────────────────────────────────

  /** Close the ChromaDB connection. */
  stop(): Promise<void>;
}

interface MemoryEntry {
  id: string;          // ChromaDB document ID
  peerId: string;
  key: string;
  value: string;
  timestamp: number;
  type?: string;
  metadata?: Record<string, string | number>;
}

interface MemorySearchResult extends MemoryEntry {
  distance: number;    // Cosine distance — lower = more similar
}
```

**Memory configuration (`MemoryConfig`):**

All read-side limits are configurable via the `MemoryConfig` object passed to `AgentMemory.create()`. These come from CLI flags, environment variables, or the preset system.

```typescript
interface MemoryConfig {
  /** Truncate entry values to this many chars (default: 10,000) */
  valueTruncationChars: number;

  /** Hard max entries returned by get()/getByPeer() (default: 50) */
  maxEntries: number;

  /** Default limit when LLM omits it (default: 10) */
  defaultLimit: number;

  /** Most recent exchange truncated to this (default: 5,000) */
  exchangeTruncationChars: number;

  /** Max total chars injected by auto-retrieve hook (default: 50,000) */
  contextBudgetChars: number;

  /** Discard search results with distance above this (default: 0.6) */
  distanceThreshold: number;

  /** Default nResults for semantic search (default: 5) */
  searchNResults: number;
}
```

**Presets — set all limits in one flag:**

| Preset | Target model | Truncation | Max entries | Budget | Exchange |
|---|---|---|---|---|---|
| `"small"` | 32K tokens | 2,000 | 20 | 12,000 | 2,000 |
| `"medium"` | 128K tokens | 5,000 | 30 | 25,000 | 3,000 |
| `"large"` (default) | 1M tokens | 10,000 | 50 | 50,000 | 5,000 |

Presets are applied by passing `--mesh-memory-preset small` (CLI) or `PI_MEMORY_PRESET=small` (env). Individual flags override specific preset values.

**Constructor logic:**
```typescript
static async create(opts): Promise<AgentMemory> {
  const host = opts.host ?? "localhost";
  const port = opts.port ?? 8000;
  const agentName = opts.agentName;
  const config = opts.config ?? PRESETS.large;

  // 1. Initialize the default embedding function (all-MiniLM-L6-v2, local WASM)
  const embedder = new DefaultEmbeddingFunction();

  // 2. Connect to ChromaDB
  const client = new ChromaClient({ host, port, ssl: false });

  // 3. Get or create the agent's collection
  const collectionName = `pi_memory_${agentName}`;
  const collection = await client.getOrCreateCollection({ 
    name: collectionName, 
    embeddingFunction: embedder 
  });

  return new AgentMemory(client, collection, collectionName, agentName, config);
}
```

**Store operation (`memory_store` / auto-save) — ChromaDB calls:**
```typescript
async store(entry): Promise<void> {
  // Append-only: every call generates a unique ID and adds a new entry.
  // The same (peerId, key) pair can accumulate many entries over a run.
  const id = `${entry.peerId}:${entry.key}:${Date.now()}:${uuidv4()}`;
  
  await this.collection.add({
    ids: [id],
    documents: [entry.value],
    metadatas: [{
      peerId: entry.peerId,
      agentName: this.agentName,
      key: entry.key,
      timestamp: Date.now(),
      type: entry.metadata?.type ?? "explicit",
      ...entry.metadata,
    }],
  });
}
```

**Semantic search (`memory_search`) — ChromaDB calls:**
```typescript
async search(query: string, opts?: { peerId?: string; nResults?: number; fullText?: boolean }): Promise<MemorySearchResult[]> {
  const n = opts?.nResults ?? this.config.searchNResults;
  
  // Query for 2× the requested results so distance filtering doesn't
  // leave the caller with fewer results than expected
  const queryN = n * 2;
  
  const where: Record<string, string> = {};
  if (opts?.peerId) where.peerId = opts.peerId;

  const results = await this.collection.query({
    queryTexts: [query],
    nResults: queryN,
    where: Object.keys(where).length > 0 ? where : undefined,
  });

  // Map ChromaDB result to MemorySearchResult[], filter by distance threshold
  const mapped: MemorySearchResult[] = [];
  for (let i = 0; i < results.ids[0].length && mapped.length < n; i++) {
    const distance = results.distances[0][i];
    if (distance > this.config.distanceThreshold) continue;
    mapped.push({
      id: results.ids[0][i],
      peerId: results.metadatas[0][i].peerId as string,
      key: results.metadatas[0][i].key as string,
      value: this.truncateValue(results.documents[0][i], opts?.fullText),
      timestamp: results.metadatas[0][i].timestamp as number,
      type: results.metadatas[0][i].type as string,
      distance,
    });
  }
  return mapped;
}
```

**Embedding function:**
- Uses `@chroma-core/default-embed` — runs the all-MiniLM-L6-v2 Sentence Transformer locally as WASM
- No external API keys required; runs entirely offline
- Embeddings are computed client-side before documents are sent to ChromaDB
- **Important:** The model has a ~256 token input limit (~400-500 words, ~2,500 chars). Text beyond this is stored but NOT embedded — vector search only considers the first ~2,500 characters of each document. The full truncated text (up to 10,000 chars) is still returned by `memory_recall` for the LLM to read. This means semantic search quality degrades for very long entries; keep `memory_store` values concise for best search results.

---

### chroma-lifecycle.ts — ChromaDB Process Management

**Path:** `src/chroma-lifecycle.ts` (NEW)  
**Exports:** `ChromaDBLifecycle` class

**Role:** Manages the ChromaDB server as a child process. Handles starting, stopping, health-checking, and binary discovery.

**Key API:**

```typescript
class ChromaDBLifecycle {
  constructor(config: { host: string; port: number; token?: string; dataPath?: string });

  isRunning(): Promise<boolean>;     // Health check via /api/v2/heartbeat
  ensureRunning(): Promise<boolean>; // Start ChromaDB if not running (localhost only)
  stop(): void;                      // SIGTERM the child process
}
```

**Binary discovery:** Searches `PATH`, `~/.local/bin/`, and platform-specific pip locations (macOS, Windows).

**Start behavior:** Spawns `chroma run --host 0.0.0.0 --path <dataDir> --port <port>`, waits up to 15s for readiness, kills on timeout.

---

### types.ts — Shared Types

**Path:** `src/types.ts`

Defines all shared TypeScript interfaces and types:

| Entity | Key Fields |
|---|---|
| `MeshPeer` | `id`, `addresses`, `status`, `agentName?`, `discoveredAt`, `disconnectedAt?` |
| `AgentRequest` | `protocol`, `requestId`, `fromAgent`, `fromPeerId`, `timestamp`, `message`, `autoReply?`, `timeoutMs?` |
| `AgentResponse` | `requestId`, `fromAgent`, `fromPeerId`, `timestamp`, `message`, `error` |
| `BroadcastMessage` | `fromAgent`, `fromPeerId`, `timestamp`, `message`, `type?` |
| `MeshConfig` | `agentName`, `listenPorts?`, `enableMdns?`, `enableDht?`, `bootstrapPeers?`, `gossipTopic?`, `announceAddresses?`, `privateKey?`, `swarmKeyPath?`, `chromaHost?`, `chromaPort?`, `memoryPreset?`, `memoryOverrides?` |
| `MeshNodeEvent` | Union: `peer:discovered`, `peer:connected`, `peer:disconnected`, `peer:identified`, `message`, `broadcast` |
| `MeshSendResult` | `peerId`, `agentName?`, `response`, `error?` |
| `MeshBroadcastResult` | `topic`, `peersReached`, `messageId` |
| `MeshDiscoverResult` | `peersFound`, `peers` |
| `MemoryEntry` (NEW) | `id`, `peerId`, `key`, `value`, `timestamp`, `type?`, `metadata?` |
| `MemorySearchResult` (NEW) | extends `MemoryEntry` + `distance: number` |

**Default config (updated with ChromaDB defaults):**

```typescript
const DEFAULT_CONFIG: Partial<MeshConfig> = {
  enableMdns: true,
  enableDht: false,
  gossipTopic: "pi-broadcast",
  listenPorts: { tcp: 0, ws: 0 },
  chromaHost: "localhost",
  chromaPort: 8000,
};
```

---

## Data Flow

### Direct Message Flow (`mesh_send`)

```
┌──────────┐     ┌──────────┐     ┌──────────────┐     ┌──────────────────┐
│ LLM      │────►│ tools.ts │────►│ protocols.ts │────►│ remote peer      │
│ invokes  │     │ execute()│     │ sendMessage()│     │ /pi-agent/0.1.0  │
│ mesh_send│     │          │     │              │     │ handler          │
└──────────┘     └──────────┘     └──────────────┘     └──────────────────┘
                                          │                      │
                                     dial peer              read stream
                                     open stream            parse CBOR
                                     write CBOR            autoReply?
                                     close write              ├─ YES: echo
                                     read response            └─ NO:  enqueue
                                     parse response                 in FIFO queue
                                          │                      │
                                          │                 wait for LLM
                                          │                 turn_end
                                          │                      │
                                          └────── CBOR Response ─┘
```

**Tool execution steps:**
1. `execute()` validates the tool parameters (the LLM already produced valid JSON per TypeBox schema)
2. Calls `meshProtocols.sendMessage(peerId, request)`
3. If dial fails, retries once (500ms delay) — two total attempts
4. Returns the response as both a formatted text block and structured details

### Broadcast Flow (`mesh_broadcast`)

```
┌──────────┐     ┌──────────┐     ┌──────────────┐     ┌──────────────────┐
│ LLM      │────►│ tools.ts │────►│ protocols.ts │────►│ GossipSub        │
│ invokes  │     │ execute()│     │ broadcast()  │     │ publish          │
│ mesh_brdx│     │          │     │              │     │                  │
└──────────┘     └──────────┘     └──────────────┘     └──────────────────┘
                                          │                      │
                                    CBOR serialize       publish on
                                    BroadcastMessage     "pi-broadcast"
                                          │               topic
                                          │                      │
                                    record in              propagate to
                                    broadcastHistory       all subscribers
                                    (capped at 200)
```

On the receiving side, each peer's `onBroadcast` callback fires, the message is recorded in the shared store, and if `autoReplyAll` is off, forwarded to the LLM via `pi.sendUserMessage()`. Additionally, the broadcast is stored in ChromaDB memory (see [Memory Persistence Flow](#memory-persistence-flow)).

### LLM Request Queue (FIFO)

The FIFO queue solves the problem of concurrent `autoReply: false` requests arriving at the same agent (H4 fix). Without it, multiple `turn_end` listeners would race.

**Data structures:**

```typescript
type PendingRequest = {
  peerId: string;
  request: AgentRequest;
  resolve: (text: string) => void;     // fulfills the awaiting mesh_send Promise
  timer: NodeJS.Timeout;               // 60s timeout
  timedOut: boolean;                   // true if timer fired before dequeue
};

const requestQueue: PendingRequest[] = [];   // FIFO queue
let activeRequest: PendingRequest | null;    // current item being processed by LLM
```

**Constants:**
- `REQUEST_TIMEOUT_MS = 60_000`
- `MAX_QUEUE_SIZE = 50`

**Flow (updated with memory retrieval):**

```
Incoming request (autoReply: false)
  │
  ├─ autoReplyAll? → YES: auto-echo, no queue
  ├─ queue full?    → YES: reject with "[queue-full]"
  │
  ├─ [NEW] Retrieve memories about requesting peer (with read-side safeguards)
  │    │
  │    ├─ agentMemory.search({ peerId, query: request.message, nResults: 3 })
  │    │   → discard results with distance > 0.6
  │    ├─ agentMemory.get(peerId, "exchange", { limit: 1 })
  │    │   → truncated to 5,000 chars
  │    ├─ Aggregate all retrieved text, cap at 50,000 chars total (~12.5K tokens)
  │    └─ If memories found, prepend memory context to request message
  │
  └─ enqueue: push PendingRequest with Promise + 60s timer
       │
       └─ advanceQueue()
            │
            ├─ activeRequest set? → return (already processing)
            ├─ skip timedOut entries at front
            ├─ dequeue → activeRequest → pi.sendUserMessage("steer")
            │
            └─ LLM responds → turn_end fires
                 │
                 ├─ extractResponseText(event.message)
                 ├─ [NEW] agentMemory.store() — save exchange as memory
                 │    key="exchange", value=[Request] + [Response]
                 ├─ resolve(activeRequest) → sends response to remote peer
                 ├─ activeRequest = null
                 └─ advanceQueue() → process next in queue
```

**Properties:**
- Only **one** `turn_end` listener is registered (once at `session_start`, never removed)
- `activeRequest` acts as a mutual exclusion lock
- Timed-out entries are resolved with `[timeout]` and skipped by `advanceQueue()`
- Backpressure: queue rejects immediately when at capacity (50 entries)
- The queue is exercised in `test-fifo-queue.mjs` with 8 test cases covering ordering, capacity, timeout, and cross-talk prevention

### Memory Persistence Flow

```
LLM interaction with peer
        │
        ▼
   [Auto-save hook in index.ts]
        │
   ┌────┴─────────────────────────────┐
   │ When?                            │
   │ • After turn_end (conversation)  │
   │ • After receiving broadcast      │
   │ • LLM calls memory_store tool    │
   └────┬─────────────────────────────┘
        │
        ▼
   AgentMemory.store({
     peerId: "12D3KooW...",
     key: "exchange",
     value: "[Request] ...\n[Response] ...",
     metadata: { type: "conversation_turn" }
   })
        │
        ▼
   ChromaDB API calls:
   1. collection.add()
      → Append new entry with unique ID ({peerId}:{key}:{ts}:{uuid4})
      → DefaultEmbeddingFunction embeds the `value` text
      → Metadata: { peerId, agentName, key, timestamp, type }
```

**ChromaDB call locations in the extension:**

| Location | Trigger | ChromaDB Call(s) |
|---|---|---|
| `index.ts` → `session_start` | Agent starts | `new ChromaClient(...)` → `client.getOrCreateCollection()` |
| `index.ts` → FIFO `turn_end` handler | LLM responds to peer request | `memory.store({ key: "exchange", ... })` → `collection.add()` |
| `index.ts` → FIFO `onRequest` handler (before enqueue) | Peer sends request | `memory.search({ peerId, query, nResults: 3 })` + `memory.get(peerId, "exchange", 1)` → `collection.query()` + `collection.get()`. Results filtered by distance ≤ 0.6, truncated, capped to 50KB total context budget. |
| `index.ts` → `onBroadcast` handler | Broadcast received | `memory.store({ key: "broadcast", ... })` → `collection.add()` |
| `tools.ts` → `memory_store` execute() | LLM explicitly saves | `memory.store(...)` → `collection.add()` |
| `tools.ts` → `memory_recall` execute() | LLM explicitly recalls | `memory.get(peerId, key, { limit, fullText })` → `collection.get()`. Values truncated to 10,000 chars unless fullText=true. Hard max 50 entries. |
| `tools.ts` → `memory_search` execute() | LLM semantic search | `memory.search(query, { peerId, nResults, fullText })` → `collection.query()`. Values truncated to 10,000 chars. distance > 0.6 filtered. |
| `tools.ts` → `memory_keys` execute() | LLM lists keys | `memory.getKeys(peerId)` → `collection.get({ where: { peerId } })` → deduplicate keys, count entries per key. |
| `index.ts` → `session_shutdown` | Agent stops | (no explicit ChromaDB close needed — client is stateless HTTP) |

### Memory Retrieval Flow

```
LLM needs context about a peer
        │
        ├── Automatic (onRequest handler)
        │       │
        │       ▼
        │   AgentMemory.search({
        │     peerId: request.fromPeerId,
        │     query: request.message,
        │     nResults: 3
        │   })
        │       │
        │       ▼
        │   ChromaDB: collection.query({ queryTexts, where: { peerId }, nResults })
        │       │
        │       ▼
        │   Prepended to user message:
        │   → "[Memory about pi-alpha:
        │       She prefers short answers.
        │       Last time we discussed project X, we decided on Y.]"
        │
        ├── LLM tool: memory_recall
        │       │
        │       ▼
        │   AgentMemory.get(peerId, key)  or  AgentMemory.getByPeer(peerId)
        │       │
        │       ▼
        │   ChromaDB: collection.get({ where: { peerId, key } })
        │
        └── LLM tool: memory_search
                │
                ▼
            AgentMemory.search(query, { peerId?, nResults })
                │
                ▼
            ChromaDB: collection.query({ queryTexts, where, nResults })
```

---

## Protocol Stack

| Layer | Library | Purpose |
|---|---|---|
| **Transport** | `@libp2p/tcp` + `@libp2p/websockets` | Connection establishment (TCP port + WebSocket) |
| **Encryption** | `@chainsafe/libp2p-noise` | End-to-end encryption (Noise protocol handshake) |
| **Multiplexing** | `@chainsafe/libp2p-yamux` | Multiple streams per connection |
| **Discovery** | `@libp2p/mdns` (always on) | Local network peer discovery |
| | `@libp2p/kad-dht` (optional) | Wide-area Kademlia-based discovery |
| | `@libp2p/bootstrap` (optional) | Bootstrap nodes for DHT |
| **Identity** | `@libp2p/identify` | Peer identity exchange (agent name, protocols) |
| **Pub/Sub** | `@chainsafe/libp2p-gossipsub` | Topic-based broadcast messaging |
| **Private Network** | `@libp2p/pnet` | Optional swarm key (PSK) for private P2P networks — all peers must share the same key |
| **Custom** | `/pi-agent/0.1.0` (custom protocol) | Direct agent-to-agent CBOR-encoded messaging |
| | `/pi-memory-host` (GossipSub topic) | ChromaDB host discovery — first node announces, others connect |
| **Memory / Vector DB** | `chromadb` + `@chroma-core/default-embed` | Persistent agent memory, semantic search, embedding storage |

**Identity:** Each agent gets an Ed25519 keypair. By default, a new keypair is generated per session, resulting in a new PeerId each time. Passing `config.privateKey` (a 32-byte seed) gives the agent **stable identity** across restarts — the PeerId is deterministically derived from the seed.

---

## Lifecycle

```
pi agent starts
  │
  ├─ Extension loads (index.ts default export)
  │    ├─ register CLI flags
  │    ├─ register commands
  │    └─ register tools (mesh + memory)
  │
  ├─ session_start event
  │    ├─ resolve agentName, config
  │    ├─ MeshNode.create(config)             → generate keypair, create libp2p
  │    ├─ new MeshProtocols(node)             → register /pi-agent/0.1.0, subscribe GossipSub
  │    ├─ wire onMessage / onRequest / onBroadcast callbacks
  │    ├─ register ONE turn_end listener (FIFO queue dequeue + memory save)
  │    ├─ node.start()                        → begin listening, discovery
  │    │
  │    ├─ 🔍 ChromaDB Host Election (see below)
  │    │    ├─ Subscribe "pi-memory-host" topic
  │    │    ├─ Check localhost:port for existing ChromaDB
  │    │    ├─ Wait 2.5s for host announcement
  │    │    ├─ If found: connect to remote host
  │    │    └─ If not: start ChromaDB locally, announce via GossipSub
  │    │
  │    ├─ AgentMemory.create(resolvedHost, resolvedPort, agentName)
  │    │    ├─ new DefaultEmbeddingFunction()
  │    │    ├─ new ChromaClient({ host, port })
  │    │    └─ client.getOrCreateCollection("pi_memory_{agentName}")
  │    │
  │    └─ setInterval(prune, 30_000)          → background stale cleanup
  │
  ├─ Agent runs
  │    ├─ LLM can call mesh tools (list_peers, send, broadcast, discover, prune)
  │    ├─ LLM can call memory tools (store, recall, search, keys)
  │    ├─ Auto-save: each peer conversation turn is persisted to ChromaDB
  │    ├─ Auto-retrieve: incoming peer requests get context prepended from memory
  │    └─ Peers can message each other
  │
  └─ session_shutdown event
       ├─ clear pruning interval
       ├─ protocols.stop()               → unhandle protocol, unsubscribe topics
       ├─ node.stop()                    → close connections, stop discovery
       ├─ chromaLifecycle.stop()         → SIGTERM ChromaDB child (if we started it)
       └─ agentMemory.stop()             → (no-op, ChromaDB client is stateless)
```

### ChromaDB Host Election

The first node to start becomes the ChromaDB host — all other agents connect to it. Discovery is via GossipSub:

```
Node A (first):   mesh start → subscribe "pi-memory-host" → 2.5s = no host found
                  → start ChromaDB → announce 192.168.1.50:8000 → connect ✓

Node B (later):   mesh start → subscribe "pi-memory-host" → receive announcement
                  → connect to 192.168.1.50:8000 ✓ (no local ChromaDB needed)
```

**Race condition:** If two nodes start simultaneously, both start ChromaDB and both announce. Later nodes connect to whichever announcement arrives first. The "extra" ChromaDB sits idle — harmless but wasteful.

**ChromaDB binding:** ChromaDB is started with `--host 0.0.0.0` so it's reachable from other machines on the network, not just localhost.

**Re-announcement:** A node that finds ChromaDB already running locally (e.g. from a previous session) re-announces itself as the host so newly joined peers can discover it.

**Manual override:** Set `--mesh-chroma-host` or `CHROMA_HOST` to point at a known ChromaDB host, skipping discovery entirely.

---

## State Management

**Three stores exist:**

| Store | Location | Persistence | Content |
|---|---|---|---|
| **Node-level peer store** | `node.ts`:`this.peerStore` | In-memory (per session) | Peers discovered via libp2p events |
| **Extension-level peer store** | `tools.ts`:`store.peers` | In-memory (per session) | Peer registry for tool responses |
| **Memory store** | `memory.ts`:`AgentMemory.collection` | **Persistent** (ChromaDB `.chroma/`) | Full chronological log of all interactions — conversations, facts, broadcasts — accumulated as append-only entries across the run |

**Why three stores?**
- **Node store:** Canonical peer state driven by libp2p connection events. Short-lived, tightly coupled to network state.
- **Extension store:** Tool-facing cache of the node store. Populated from node events, with eventual consistency. Cleared on shutdown.
- **Memory store:** Persistent, cross-session. Survives agent restarts. Uses vector embeddings for semantic retrieval. Keyed by `(peerId, key)`.

**Memory persistence across sessions:**
- ChromaDB data is stored on disk at `.chroma/` (configurable via `chroma run --path`)
- The memory collection persists even when the pi agent restarts
- On restart, `AgentMemory.create()` reconnects to the existing collection — all memories are immediately available
- This means the agent "remembers" past conversations across sessions

**Broadcast history:**
- In-memory only, capped at `MAX_BROADCAST_HISTORY = 200` entries
- Broadcasts are also persisted to ChromaDB memory (key: `"broadcast"`) for longer-term recall

---

## Design Decisions & Fixes

These fixes were identified in the [OPTIMIZATION-REPORT.md](./OPTIMIZATION-REPORT.md). Items **H1–H4** (high severity) and **M1–M3** (medium severity) were subsequently implemented.

| Label | Severity | File | Issue | Solution |
|---|---|---|---|---|
| **H1** | 🔴 HIGH | `node.ts` | Auto-dial storms — every mDNS discovery fired a separate `setTimeout(dial)` | Debounced batching: collect in `pendingDials` Set, dial all in one batch after 200ms coalescing window |
| **H2** | 🔴 HIGH | `node.ts` | `getPeers()` only returned connected peers — disconnected peers invisible | Internal `Map<string, MeshPeer>` persists peers across connect/disconnect, backfills from libp2p connections |
| **H3** | 🔴 HIGH | `protocols.ts` | `readStream()` didn't propagate AbortSignal — indefinite hang on unresponsive peers | Pass `AbortController.signal` through to `readStream()` and check between stream chunks |
| **H4** | 🔴 HIGH | `index.ts` | Concurrent `onRequest` raced on `turn_end` — multiple listeners leaked | Single global `turn_end` listener + `activeRequest` lock + `requestQueue` FIFO |
| **M1** | 🟡 MEDIUM | `node.ts` | Fresh keypair every restart — identity churn, stale peer accumulation | Accept optional `privateKey` (32-byte seed) in `MeshConfig` for stable identity |
| **M2** | 🟡 MEDIUM | `protocols.ts` | `handleIncomingMessage` swallowed errors silently — sender waited full timeout | Write error `AgentResponse` with `error: true` so sender fails fast (<100ms) |
| **M3** | 🟡 MEDIUM | `tools.ts` | `broadcastHistory` array grew unbounded — memory leak | Capped at 200 entries, oldest evicted on push |

---

## Configuration & CLI Flags

| Flag | Type | Default | Description |
|---|---|---|---|
| `--agent-name` | string | `pi-<hostname>` | Agent name for the mesh (also from `PI_MESH_NAME` or `PI_COMM_NAME` env var) |
| `--mesh-enable-dht` | boolean | `false` | Enable Kademlia DHT for wide-area discovery |
| `--mesh-gossip-topic` | string | `pi-broadcast` | GossipSub topic for broadcast messages |
| `--mesh-swarm-key` | string | — | Path to a `swarm.key` file for private P2P network (PSK). All peers must share the same key. |
| `--mesh-chroma-host` | string | `localhost` | ChromaDB server hostname (or `CHROMA_HOST` env var) |
| `--mesh-chroma-port` | number | `8000` | ChromaDB server port (or `CHROMA_PORT` env var) |
| `--mesh-chroma-token` | string | — | Auth token for ChromaDB (`x-chroma-token` header) |
| `--mesh-chroma-data-path` | string | `~/.local/share/chroma` | ChromaDB data directory for persistence (or `CHROMA_DATA_PATH` env var) |
| `--mesh-memory-preset` | string | `large` | Memory limit preset: `small` (32K), `medium` (128K), `large` (1M). Sets all read-side limits at once. |
| `--mesh-memory-max-entries` | number | — | Override hard max entries (50 for large preset) |
| `--mesh-memory-truncate` | number | — | Override value truncation in chars (10,000 for large preset) |
| `--mesh-memory-budget` | number | — | Override auto-retrieve context budget in chars (50,000 for large preset) |
| `--mesh-memory-exchange-truncate` | number | — | Override exchange truncation in chars (5,000 for large preset) |
| `--mesh-memory-distance` | float | — | Override distance threshold (0.6 default) |

**Environment variables:**
- `PI_MESH_NAME` — agent name (overrides default, but CLI flag takes priority)
- `PI_COMM_NAME` — backward-compatible alias for agent name
- `PI_SWARM_KEY` — path to a `swarm.key` file (alternative to `--mesh-swarm-key` flag)
- `CHROMA_HOST` — ChromaDB host (alternative to `--mesh-chroma-host`)
- `CHROMA_PORT` — ChromaDB port (alternative to `--mesh-chroma-port`)
- `CHROMA_TOKEN` — ChromaDB auth token (alternative to `--mesh-chroma-token`)
- `CHROMA_DATA_PATH` — ChromaDB data directory (alternative to `--mesh-chroma-data-path`)
- `PI_MEMORY_PRESET` — memory limit preset: `small`, `medium`, `large` (alternative to `--mesh-memory-preset`)
- `PI_MEMORY_MAX_ENTRIES` — override max entries (alternative to `--mesh-memory-max-entries`)
- `PI_MEMORY_TRUNCATE` — override truncation chars (alternative to `--mesh-memory-truncate`)
- `PI_MEMORY_BUDGET` — override context budget chars (alternative to `--mesh-memory-budget`)

**Config priority:** CLI flag → environment variable → default

**Usage examples:**

```bash
# Use the "small" preset for a 32K-token model
pi --mesh-memory-preset small

# Use "medium" preset but double the context budget
pi --mesh-memory-preset medium --mesh-memory-budget 50000

# Individual overrides without a preset (large defaults apply to remaining)
pi --mesh-memory-max-entries 100 --mesh-memory-truncate 20000

# Environment variable approach
export PI_MEMORY_PRESET=small
export PI_MEMORY_MAX_ENTRIES=100
pi
```

---

## Commands

| Command | Role | Description |
|---|---|---|
| `/mesh-auto-reply [on\|off]` | Toggle | When on, all incoming mesh messages auto-echo without involving the LLM. No argument toggles current state. |
| `/mesh-list-peers` | Read | List all known peers with connection status, agent name, age |
| `/mesh-discover` | Read | Scan for recently discovered peers |
| `/mesh-prune` | Write | Remove all disconnected/stale peers immediately |

---

## Test Suite

All tests located in the project root (`*.mjs`). Run with `npm test` (requires build) or individual `node test-*.mjs`.

| Suite | File | Count | Type | Network Required |
|---|---|---|---|---|
| Comprehensive | `test-network.mjs` | 26 tests | Integration | ✅ (needs peers) |
| Rigorous | `rigorous-concurrent-test.mjs` | 82 tests (10 phases) | Stress/load | ✅ (needs peers) |
| FIFO Queue | `test-fifo-queue.mjs` | 8 tests | Unit | ❌ |
| Memory/Leak | `test-leak.mjs` | 7 tests | Unit | ❌ |
| Stable Identity | `test-identity.mjs` | 9 tests | Unit | ❌ |
| Negative/Edge | `test-negative.mjs` | 11 tests | Fuzzing | ✅ (needs peers) |
| Extension Import | `test-extension.mjs` | 1 test | Smoke | ❌ |
| **Memory** (NEW) | `test-memory.mjs` | TBD | Unit + Integration | ❌ (needs ChromaDB) |

**Key performance metrics (rigorous test, 4-peer mesh):**
- Peak throughput: 741–781 msg/s
- Average latency: 2.6–2.7ms
- p95 latency: 4–6ms
- Max payload tested: 100KB
- Zero message loss across 148 total messages in 2 runs

---

## Extension Integration

The extension is discovered by pi via the `package.json` `pi` field:

```json
{
  "pi": {
    "extensions": ["./index.ts"],
    "image": "https://raw.githubusercontent.com/libp2p/js-libp2p/main/img/libp2p-logo.png"
  }
}
```

The `index.ts` at the project root simply re-exports the compiled output:

```typescript
export { default } from './dist/index.js';
```

**Peer dependencies:**
- `@earendil-works/pi-coding-agent` — the pi agent framework providing `ExtensionAPI`
- `typebox` — JSON schema validation for tool parameters

**Runtime dependencies (NEW):**
- `chromadb` — ChromaDB JS client (`^3.4.3`)
- `@chroma-core/default-embed` — Default embedding function (`^0.1.9`)

**Build:** TypeScript compiled to `dist/` (ES2022, NodeNext module resolution, strict mode).

---

## ChromaDB Memory Layer

### Overview

The ChromaDB memory layer gives each pi agent persistent, semantic memory of its interactions. Every conversation turn with a peer, every broadcast received, and every explicit `memory_store` call is stored as a vector-embedded document in ChromaDB. The LLM can later search these memories by semantic similarity (natural language query) or retrieve them by exact `(peerId, key)` lookup.

### Data Model

Each memory entry in ChromaDB:

```typescript
{
  id: "12D3KooW...abc:prefs:1716587000000:a1b2c3d4",
  document: "This agent prefers short, code-only responses with no explanations.",
  metadata: {
    peerId: "12D3KooW...abc",
    agentName: "pi-alpha",     // our own agent name
    key: "prefs",              // user-defined key
    timestamp: 1716587000000,
    type: "explicit"           // "conversation_turn" | "broadcast" | "explicit" | "system"
  },
  embedding: [0.0123, -0.0456, ...]  // 384-dim all-MiniLM-L6-v2
}
```

**Key design principles:**
1. **Embedded on `document` (value)**: The text content is what gets vector-embedded. Metadata is for filtering, not embedding.
2. **Metadata for scoping**: `peerId` and `key` are metadata fields that ChromaDB can filter on efficiently.
3. **Append-only log**: Every `store()` call creates a new entry with a unique ID (`{peerId}:{key}:{timestamp}:{uuid4}`). Entries accumulate chronologically — the same `(peerId, key)` pair can have many entries over time, forming a complete interaction log for the duration of the run.
4. **Persistent across sessions**: Data survives agent restarts because ChromaDB persists to disk.
5. **Collection per agent**: `pi_memory_{agentName}` keeps each agent's memory isolated.

### Storage Growth (Append-Only Log)

Since every interaction appends a new entry (never overwrites), storage grows linearly with activity during a run.

| Activity | Entries per event |
|---|---|
| Each peer exchange (LLM turn) | 1 entry (key: `"exchange"`) |
| Each broadcast received | 1 entry (key: `"broadcast"`) |
| Each explicit `memory_store` call | 1 entry (LLM-defined key) |

**Per-run estimates:**

| Scenario | Peers | Exchanges/peer | Broadcasts | Explicit stores | Total entries |
|---|---|---|---|---|---|
| Light (1 peer, brief task) | 1 | 5 | 2 | 3 | **~10** |
| Typical (3 peers, normal task) | 3 | 10 | 5 | 6 | **~40** |
| Heavy (5 peers, deep collaboration) | 5 | 20 | 10 | 10 | **~120** |
| Stress (10 peers, extended session) | 10 | 30 | 15 | 20 | **~335** |

At ~1–4KB per entry (document text + 384-dim embedding + metadata), a typical session consumes **<1MB**. Even stress scenarios stay under ~2MB. ChromaDB can handle millions of entries comfortably, so append-only growth is not a concern for normal use.

Retrieving the *most recent* entries for a given `(peerId, key)` uses `collection.get()` with a `limit` parameter — ChromaDB returns entries sorted by insertion order, so the newest N are always accessible without a full scan.

### Read-Side Analysis

Every read path pulls data back into the LLM's context window. Some paths are bounded, some are not:

#### Read Path 1: Auto-retrieve on incoming request (EVERY peer message)

Triggered automatically in `onRequest` — this is the **hottest path** and the most dangerous:

```
memory.search({ peerId, query: request.message, nResults: 3 })   → 3 entries
memory.get(peerId, "exchange", 1)                                  → 1 entry
```

**Risk:** The most recent exchange entry (`"exchange"`) could be huge — a code review, a long explanation, or a detailed plan easily exceeds 5KB. Plus 3 semantic search results. Every incoming message from a peer burns 4 memory entries into the LLM's system context.

| Session age | Exchange count with peer | Search results | Injected into LLM context |
|---|---|---|---|
| Early (3 exchanges) | Small entries (~500B each) | 3 short entries | ~2KB — fine |
| Mid (12 exchanges) | Medium entries (~1KB each) | 3 mixed entries | ~5KB — acceptable |
| Late (30+ exchanges) | Large entries (2-5KB each) | 3 large entries | **12-20KB** — problematic |

#### Read Path 2: `memory_recall` tool (LLM-triggered)

```
memory.get(peerId, key, limit)
```

**Risk:** The `limit` parameter is optional. If `limit` is omitted or set high, the LLM gets EVERY entry for that `(peerId, key)` pair. A peer with 30 `"exchange"` entries at 1-5KB each returns **30-150KB** of text.

#### Read Path 3: `memory_search` tool (LLM-triggered)

```
memory.search(query, { peerId?, nResults })
```

**Risk: LOW** — `nResults` defaults to 5. Bounded and LLM-controlled. 5 × 1-5KB = 5-25KB, manageable.

#### Read Path 4: `getByPeer` (available in API, unused by tools currently)

**Risk: HIGH** — Returns ALL entries for a peer across all keys with no built-in limit. Not exposed as an LLM tool, but available in the API.

### Read-Side Mitigations

To prevent context window blowout without sacrificing the append-only log, the following safeguards are applied at the `AgentMemory` layer:

| Mitigation | Applies to | Description |
|---|---|---|
| **Value truncation** | `get()`, `search()`, `getByPeer()` | Each entry's `value` is truncated to `valueTruncationChars` before returning (default 10,000). Full values retrievable with `fullText: true`. Configure via `--mesh-memory-truncate` or preset. |
| **Hard max limit** | `get()`, `getByPeer()` | At most `maxEntries` returned regardless of `limit` param (default 50). Configure via `--mesh-memory-max-entries` or preset. |
| **Distance threshold** | auto-retrieve `search()` | Results with `distance > distanceThreshold` (default 0.6) are discarded. Configure via `--mesh-memory-distance` or preset. |
| **Exchange truncation** | auto-retrieve `get("exchange")` | Most recent exchange truncated to `exchangeTruncationChars` (default 5,000). Configure via `--mesh-memory-exchange-truncate` or preset. |
| **Max context budget** | auto-retrieve hook | Total auto-injected memory text capped at `contextBudgetChars` (default 50,000, ~12.5K tokens). Configure via `--mesh-memory-budget` or preset. |

**Resulting read budgets under mitigations:**

| Read path | Without mitigations | With mitigations |
|---|---|---|
| Auto-retrieve (hot path) | Up to 20KB per incoming message | **≤contextBudgetChars** (default 50KB, ~12.5K tokens) — <2% of a 1M window |
| `memory_recall` (LLM tool) | Up to 150KB (unbounded) | **≤maxEntries × truncation** (default 500KB, ~125K tokens) — ~12% of a 1M window |
| `memory_search` (LLM tool) | Up to 25KB | **≤searchNResults × truncation** (default 50KB, ~12.5K tokens) |

In a 1M token context window (~4M chars), the worst-case read path (`memory_recall` at 500KB / 125K tokens) consumes only ~12% of the window. The auto-retrieve hot path consumes ~1%. These limits are generous enough that `fullText: true` should rarely be needed.

> **Note:** The limits above are designed for 1M-token-class models (Gemini 2.5 Pro, Claude, GPT-4o). If targeting smaller models (32K–128K tokens), reduce the context budget to 12,000 chars and hard max to 20 entries.

**Default:** `@chroma-core/default-embed` — the `all-MiniLM-L6-v2` Sentence Transformer model.

| Property | Value |
|---|---|
| Dimensions | 384 |
| Model | all-MiniLM-L6-v2 (Sentence Transformers) |
| Runtime | WASM (runs locally in Node.js) |
| Max sequence length | 256 tokens |
| External API required | No |

The embedding function is initialized once at `AgentMemory.create()` time and passed to the collection. All subsequent `add` and `query` operations automatically compute embeddings client-side before sending to the ChromaDB server.

### Collection Lifecycle

```
AgentMemory.create(host, port, "pi-alpha")
  │
  ├─ new DefaultEmbeddingFunction()     // Initialize WASM embedding model
  ├─ new ChromaClient({ host, port })   // Connect to ChromaDB HTTP API
  └─ client.getOrCreateCollection({ 
       name: "pi_memory_pi-alpha", 
       embeddingFunction: embedder 
     })
       │
       ├─ If collection exists → reconnect, all memories available
       └─ If new → create empty collection with embedding function
```

### Auto-Save Strategy

The extension automatically saves memories at these points — no LLM action required:

| Trigger | Key | Type | Value |
|---|---|---|---|
| LLM responds to a peer (turn_end) | `"exchange"` | `"conversation_turn"` | `[Request] {text}\n[Response] {text}` |
| Broadcast received (onBroadcast) | `"broadcast"` | `"broadcast"` | `[{type}] {message}` |

**Append-only logging:** Every exchange with a peer creates a new `"exchange"` entry — nothing is overwritten. Over the course of a run, this builds a complete chronological record of all interactions. The LLM can retrieve the most recent N exchanges with `memory_recall(peerId, "exchange", limit=N)` or semantically search the full history with `memory_search`. Storage grows linearly with session activity — a typical multi-peer session produces ~50–200 entries total.

**Outgoing exchanges:** When the LLM initiates a conversation via `mesh_send`, the outgoing message + peer's response is also auto-saved by the `mesh_send` tool's `execute()` method after receiving the response. This ensures both sides of every conversation are captured.

**Fault tolerance:** All ChromaDB calls in auto-save hooks are wrapped in try/catch with debug-level logging. If ChromaDB is unreachable mid-session, the auto-save is silently skipped — mesh communication continues uninterrupted. Memory tools return clear error messages to the LLM so it can adjust its strategy.

### Auto-Retrieve Strategy

When a peer sends a direct message (autoReply=false), the extension automatically:

1. **Semantic search**: Finds the 3 most relevant memories using the incoming message as a query (filtered: distance > 0.6 discarded)
2. **Last exchange**: Gets the most recent `"exchange"` entry for that peer (truncated to 5,000 chars)
3. **Context budget**: Total injected memory text is capped at **50,000 characters** (~12.5K tokens) — entries are trimmed shortest-first if the sum exceeds this
4. **Prepends context**: Formatted and prepended to the user message before it reaches the LLM

Format of the auto-retrieved context:
```
[Memory about pi-alpha:
  She prefers short, code-only responses with no explanations.
  Last exchange — Request: "how do i connect two libp2p nodes?"
  Response: "Use dialProtocol() and handle(). Here's a minimal example: ..."
  We've interacted 12 times total.]
```

This gives the LLM immediate awareness of who it's talking to and what they discussed before.

### LLM-Callable Memory Tools

Three new tools give the LLM explicit control over memory:

#### `memory_store` — Save a key-value memory

```
Parameters:
  peerId: string    — Peer this memory is about
  key: string       — Category/name (e.g., "project_context", "prefs", "decision")
  value: string     — The content to remember
  metadata?: object — Optional additional metadata
```

The LLM can use this to explicitly remember facts:
```
"memory_store: peerId=12D3..., key=decision, value=We agreed to use TypeScript strict mode for this project"
```

#### `memory_recall` — Recall by peer and key

At least one of `peerId` or `key` is required — you can't recall an unfiltered global dump.

```
Parameters:
  peerId?: string   — Filter by peer (required if key is omitted)
  key?: string      — Filter by key (required if peerId is omitted)
  limit?: number    — Max results (default 10, hard max 50)
  fullText?: bool   — Return full untruncated values (default false — truncated to 10,000 chars)
```

**Read budget:** At most 50 entries × 10,000 chars = 500KB (~125K tokens). Typical use (10 entries × 2,000 chars) = ~20KB (~5K tokens).

Returns memories as formatted text with timestamps:
```

🔑 **decision** (2h ago)
We agreed to use TypeScript strict mode for this project.

🔑 **prefs** (5h ago)
She prefers short, code-only responses with no explanations.
```

#### `memory_search` — Semantic search

```
Parameters:
  query: string     — Natural language search query
  peerId?: string   — Optional: scope to a specific peer
  nResults?: number — Max results (default 5)
  fullText?: bool   — Return full untruncated values (default false — truncated to 10,000 chars)
```

**Read budget:** 5 entries × 10,000 chars = 50KB max (~12.5K tokens). Entries with distance > 0.6 are excluded.

Returns results ranked by semantic similarity, with distance scores:
```
**3 memories similar to "project settings":**

1. 🟢 **decision** (distance: 0.23) — pi-alpha — 2h ago
   We agreed to use TypeScript strict mode for this project.

2. 🟡 **project_context** (distance: 0.38) — pi-beta — 30m ago
   Project uses React 18 with Vite bundler and Material UI.

3. 🟡 **prefs** (distance: 0.41) — pi-alpha — 5h ago
   She prefers short, code-only responses with no explanations.
```

#### `memory_keys` — List keys for a peer

```
Parameters:
  peerId: string    — Peer to list keys for
```

Returns all unique keys stored for this peer, with counts:
```
**Keys for pi-alpha (8 total entries):**

  🔑 exchange (5 entries)
  🔑 prefs (1 entry)
  🔑 decision (1 entry)
  🔑 todo (1 entry)
```

Use this before `memory_recall` to discover what information is available.

### Prompt Guidelines for Memory Tools

These are injected into the LLM's system context via each tool's `promptGuidelines`:

- **`memory_store`**: "Use memory_store to remember facts, decisions, preferences, or context about peers. Save key information after each meaningful conversation turn — especially when a peer shares preferences, makes decisions, or provides important context. See AGENT-MEMORY.md for usage patterns and anti-patterns."
- **`memory_recall`**: "Use memory_recall when preparing to interact with a peer to check what you already know about them. Use mesh_list_peers first to get active peer IDs. Auto-retrieve already injects the latest exchange + 3 search results, so use this for deeper context or specific key lookups."
- **`memory_search`**: "Use memory_search to find relevant past conversations by meaning rather than by exact key. Best for cross-cutting queries like 'what decisions have we made about performance?'"
- **`memory_keys`**: "Use memory_keys to discover what categories of information you've stored about a peer before recalling specific entries."

---

## AGENT-MEMORY.md — LLM Usage Guide

A companion document at [`AGENT-MEMORY.md`](./AGENT-MEMORY.md) teaches the LLM:

- **What happens automatically** (exchange logging, broadcast recording, auto-retrieve on incoming messages)
- **When to use `memory_store`** (decisions, preferences, constraints, observations — not raw conversation)
- **When to use `memory_recall`** (before high-stakes responses, checking past decisions, resuming after gaps)
- **When to use `memory_search`** (cross-cutting semantic queries across all peers)
- **Key naming conventions** (short, semantic, reusable keys; put detail in values)
- **Common patterns** (first contact, reconnecting, collaborative decisions, async task tracking)
- **Anti-patterns** (don't store every exchange manually, don't use unique-per-turn keys, don't store secrets)

The `promptGuidelines` on each memory tool reference this document so the LLM knows where to find detailed usage guidance.

---

## Future Considerations

1. **NAT traversal** — Relay-based NAT traversal or hole-punching could extend the mesh beyond the local network.

2. **Peer scoring / reputation** — A scoring mechanism based on response latency, reliability, and uptime could inform routing decisions. Could be stored in ChromaDB.

3. **Protocol versioning** — A version negotiation mechanism would enable forward/backward compatibility.

4. **On-demand peer discovery** — The current `mesh_discover` tool is passive (filters known peers by age). An active discovery mechanism (DHT query, mDNS re-publish) would provide real scanning capability.

5. **Run-level memory scoping** — Memories accumulate indefinitely per run. Adding a per-run namespace or automatic cleanup on session shutdown would let users isolate sessions without needing to manage the ChromaDB collection manually.

6. **Configurable embedding models** — Support pluggable embedding functions (OpenAI embeddings, Cohere, custom models) for different quality/speed tradeoffs.

7. **Memory TTL / expiration** — Automatic expiry of old memories to prevent unbounded storage growth.

8. **Memory compression** — Periodically summarize old conversation turns into compact facts using the LLM itself.

9. **Cross-agent memory sharing** — Peers could exchange memory digests during the identify handshake, giving each agent a "gossip-based" understanding of what other agents know about the network.

10. **Parallel ChromaDB connections** — For agents that interact with hundreds of peers, batch ChromaDB operations and connection pooling would reduce latency.


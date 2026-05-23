# pi-libp2p-mesh — Architecture

> **Last updated:** 2026-05-23  
> **Package:** `@earendil-works/pi-libp2p-mesh` v0.3.0  
> **Lines of code:** ~1,680 TypeScript across 5 source files

---

## Table of Contents

1. [Overview](#overview)
2. [High-Level Architecture](#high-level-architecture)
3. [Module Breakdown](#module-breakdown)
   - [index.ts — Extension Entry Point & Lifecycle](#indexts--extension-entry-point--lifecycle)
   - [node.ts — MeshNode (libp2p Node)](#nodets--meshnode-libp2p-node)
   - [protocols.ts — MeshProtocols (Messaging)](#protocolsts--meshprotocols-messaging)
   - [tools.ts — LLM Tools](#toolsts--llm-tools)
   - [types.ts — Shared Types](#typests--shared-types)
4. [Data Flow](#data-flow)
   - [Direct Message Flow (`mesh_send`)](#direct-message-flow-mesh_send)
   - [Broadcast Flow (`mesh_broadcast`)](#broadcast-flow-mesh_broadcast)
   - [LLM Request Queue (FIFO)](#llm-request-queue-fifo)
5. [Protocol Stack](#protocol-stack)
6. [Lifecycle](#lifecycle)
7. [State Management](#state-management)
8. [Design Decisions & Fixes](#design-decisions--fixes)
9. [Configuration & CLI Flags](#configuration--cli-flags)
10. [Commands](#commands)
11. [Test Suite](#test-suite)
12. [Extension Integration](#extension-integration)
13. [Future Considerations](#future-considerations)

---

## Overview

`pi-libp2p-mesh` is a **pi extension** that creates a peer-to-peer overlay network for pi coding agents. Built on [libp2p](https://libp2p.io/) v3, it enables:

- **Automatic peer discovery** via mDNS (local network) and optional Kademlia DHT (wide-area)
- **Direct agent-to-agent messaging** over a custom libp2p stream protocol (`/pi-agent/0.1.0`)
- **GossipSub broadcast** for group announcements and coordination
- **End-to-end encryption** via the Noise protocol
- **Five LLM-callable tools** for the agent to orchestrate P2P workflows

The extension runs as a singleton within each pi agent process. Every agent that loads this extension becomes a mesh peer with a unique Ed25519-based PeerId.

---

## High-Level Architecture

```
┌───────────────────────────────────────────────────────────┐
│                     pi agent process                      │
│  ┌─────────────────────────────────────────────────────┐  │
│  │            pi-libp2p-mesh extension                 │  │
│  │                                                     │  │
│  │  ┌───────────────┐    ┌────────────────┐            │  │
│  │  │   index.ts    │◄──►│   tools.ts     │            │  │
│  │  │  (lifecycle,  │    │  (5 LLM tools, │            │  │
│  │  │   FIFO queue, │    │   peer store,  │            │  │
│  │  │   event bus,  │    │   pruning)     │            │  │
│  │  │   commands)   │    └───────┬────────┘            │  │
│  │  └───────┬───────┘           │                      │  │
│  │          │            ┌──────▼────────┐             │  │
│  │          └───────────►│ MeshProtocols │             │  │
│  │                       │  · sendMessage│             │  │
│  │                       │  · broadcast  │             │  │
│  │                       │  · gossip sub |             │  │
│  │                       └──────┬─────── ┘             │  │
│  │                              │                      │  │
│  │                       ┌──────▼────────┐             │  │
│  │                       │   MeshNode    │             │  │
│  │                       │  · libp2p     │             │  │
│  │                       │  · transports │             │  │
│  │                       │  · discovery  │             │  │
│  │                       │  · peer store │             │  │
│  │                       └───────────────              │  │
│  └─────────────────────────────────────────────────────┘  │
│                         │                                 │
│              ┌──────────┴──────────┐                      │
│              │   libp2p overlay    │                      │
│              │  TCP + WebSocket    │                      │
│              │  Noise / Yamux      │                      │
│              │  mDNS + DHT         │                      │
│              └─────────────────────┘                      │
└───────────────────────────────────────────────────────────┘
```

### Dependency Graph

```
index.ts
  ├── node.ts ───────── types.ts
  ├── protocols.ts ──── types.ts
  └── tools.ts ──────── types.ts, protocols.ts (type only)
```

---

## Module Breakdown

### index.ts — Extension Entry Point & Lifecycle

**Path:** `src/index.ts`  
**Role:** The glue layer — wires all modules together and interfaces with the pi `ExtensionAPI`.

**Responsibilities:**

1. **CLI Flag Registration** — registers `--agent-name`, `--mesh-enable-dht`, `--mesh-gossip-topic`
2. **`session_start` handler** — the boot sequence:
   - Resolves agent name (CLI flag → `PI_MESH_NAME` env var → `PI_COMM_NAME` env var → `pi-<hostname>`)
   - Creates `MeshNode` (libp2p node)
   - Creates `MeshProtocols` (message handler)
   - Sets up the **FIFO LLM request queue** (see [LLM Request Queue](#llm-request-queue-fifo))
   - Wires event handlers (peer discovery, connect, disconnect, identify, message, broadcast)
   - Wires inbound broadcasts to `pi.sendUserMessage()` so the LLM sees them
   - Starts background stale-peer pruning (every 30s)
3. **`session_shutdown` handler** — stops the node, cancels pruning timer, tears down protocols
4. **Command Registration** — `/auto-reply`, `/mesh-list-peers`, `/mesh-discover`, `/mesh-prune`
5. **Delegates tool registration** to `registerMeshTools()` in `tools.ts`

**Singleton state:**

| Variable | Type | Purpose |
|---|---|---|
| `meshNode` | `MeshNode \| null` | Active libp2p node |
| `meshProtocols` | `MeshProtocols \| null` | Active protocol handler |
| `pruneInterval` | `setInterval` handle | Background pruning timer |
| `store` | `MeshStore` | Shared peer registry, broadcast history, agent name, auto-reply flag |

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
  // Send a direct message to a peer, await their response
  sendMessage(peerId: string, request: Omit<AgentRequest, 'fromPeerId' | 'timestamp'>): Promise<AgentResponse>;

  // Publish a broadcast to all peers
  broadcast(message: Omit<BroadcastMessage, 'fromPeerId' | 'timestamp'>): Promise<MeshBroadcastResult>;

  // Callbacks — set by index.ts
  onMessage: (peerId: string, request: AgentRequest) => void;
  onBroadcast: (msg: BroadcastMessage) => void;
  onRequest: (peerId: string, request: AgentRequest) => Promise<string>;

  // Lifecycle
  stop(): Promise<void>;
}
```

**Direct messaging protocol (`/pi-agent/0.1.0`):**

Handshake is a simple JSON request-response over a libp2p stream:

1. **Sender:** Opens a stream, writes a JSON `AgentRequest`, closes write side
2. **Receiver:** Reads the stream, processes the request, writes a JSON `AgentResponse`
3. **Sender:** Reads the response, parses it, returns it to the caller

The protocol includes a **60-second default timeout** (configurable per-request via `timeoutMs`) managed by an `AbortController`. A **retry loop** (2 attempts, 500ms delay) handles transient dial failures.

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

- Publishes JSON-serialized `BroadcastMessage` to the configured topic
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
**Exports:** `registerMeshTools`, `setMeshProtocols`, `listPeers`, `pruneAllDisconnected`, `pruneStalePeers`, `recordBroadcast`, `MeshStore`, `PeerListResult`

**Role:** Registers the five custom tools that the LLM can invoke, and manages the shared `MeshStore` state.

**Five LLM Tools:**

| Tool | Description | Parameters |
|---|---|---|
| `mesh_list_peers` | List all known peers with status, addresses, agent names | `{}` |
| `mesh_send` | Send a direct message to a peer, await their response | `peerId: string`, `message: string`, `autoReply?: boolean` |
| `mesh_broadcast` | Publish a GossipSub broadcast | `message: string`, `type?: "announce" \| "query" \| "response" \| "event"` |
| `mesh_discover` | Scan for new peers, report recently discovered | `{}` |
| `mesh_prune` | Remove all disconnected/stale peers | `{}` |

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

### types.ts — Shared Types

**Path:** `src/types.ts`

Defines all shared TypeScript interfaces and types:

| Entity | Key Fields |
|---|---|
| `MeshPeer` | `id`, `addresses`, `status`, `agentName?`, `discoveredAt`, `disconnectedAt?` |
| `AgentRequest` | `protocol`, `requestId`, `fromAgent`, `fromPeerId`, `timestamp`, `message`, `autoReply?`, `timeoutMs?` |
| `AgentResponse` | `requestId`, `fromAgent`, `fromPeerId`, `timestamp`, `message`, `error` |
| `BroadcastMessage` | `fromAgent`, `fromPeerId`, `timestamp`, `message`, `type?` |
| `MeshConfig` | `agentName`, `listenPorts?`, `enableMdns?`, `enableDht?`, `bootstrapPeers?`, `gossipTopic?`, `announceAddresses?`, `privateKey?` |
| `MeshNodeEvent` | Union: `peer:discovered`, `peer:connected`, `peer:disconnected`, `peer:identified`, `message`, `broadcast` |
| `MeshSendResult` | `peerId`, `agentName?`, `response`, `error?` |
| `MeshBroadcastResult` | `topic`, `peersReached`, `messageId` |
| `MeshDiscoverResult` | `peersFound`, `peers` |

**Default config:**

```typescript
const DEFAULT_CONFIG: Partial<MeshConfig> = {
  enableMdns: true,
  enableDht: false,
  gossipTopic: "pi-broadcast",
  listenPorts: { tcp: 0, ws: 0 },   // random ports
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
                                     open stream            parse JSON
                                     write JSON             autoReply?
                                     close write              ├─ YES: echo
                                     read response            └─ NO:  enqueue
                                     parse response                 in FIFO queue
                                          │                      │
                                          │                 wait for LLM
                                          │                 turn_end
                                          │                      │
                                          └────── JSON Response ─┘
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
                                    JSON serialize        publish on
                                    PendingRequest       "pi-broadcast"
                                          │               topic
                                          │                      │
                                    record in              propagate to
                                    broadcastHistory       all subscribers
                                    (capped at 200)
```

On the receiving side, each peer's `onBroadcast` callback fires, the message is recorded in the shared store, and if `autoReplyAll` is off, forwarded to the LLM via `pi.sendUserMessage()`.

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

**Flow:**

```
Incoming request (autoReply: false)
  │
  ├─ autoReplyAll? → YES: auto-echo, no queue
  ├─ queue full?    → YES: reject with "[queue-full]"
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
| **Custom** | `/pi-agent/0.1.0` (custom protocol) | Direct agent-to-agent JSON messaging |

**Identity:** Each agent gets an Ed25519 keypair. By default, a new keypair is generated per session, resulting in a new PeerId each time. Passing `config.privateKey` (a 32-byte seed) gives the agent **stable identity** across restarts — the PeerId is deterministically derived from the seed.

---

## Lifecycle

```
pi agent starts
  │
  ├─ Extension loads (index.ts default export)
  │    ├─ register CLI flags
  │    ├─ register commands
  │    └─ register tools
  │
  ├─ session_start event
  │    ├─ resolve agentName
  │    ├─ MeshNode.create(config)     → generate keypair, create libp2p
  │    ├─ new MeshProtocols(node)     → register /pi-agent/0.1.0, subscribe GossipSub
  │    ├─ wire onMessage / onRequest / onBroadcast callbacks
  │    ├─ register ONE turn_end listener (FIFO queue dequeue)
  │    ├─ node.start()                → begin listening, discovery
  │    └─ setInterval(prune, 30_000)  → background stale cleanup
  │
  ├─ Agent runs (LLM can call tools, peers can message each other)
  │
  └─ session_shutdown event
       ├─ clear pruning interval
       ├─ protocols.stop()            → unhandle protocol, unsubscribe topic
       └─ node.stop()                 → close connections, stop discovery
```

---

## State Management

**Two peer stores exist:**

1. **Node-level** (`node.ts`:`this.peerStore: Map<string, MeshPeer>`)
   - Populated by libp2p events (peer:discovery, peer:connect, peer:disconnect, peer:identify)
   - Maintains disconnected peers with timestamps
   - Queried by `getPeers()`, `pruneStalePeers()`, `pruneAllDisconnected()`

2. **Extension-level** (`tools.ts`:`store.peers: Map<string, MeshPeer>`)
   - Populated by `handleNodeEvent()` in `index.ts`, which receives events from the node-level store
   - Used by tools (`mesh_list_peers`, `mesh_discover`, `mesh_prune`) and commands
   - Pruning operates on this store

**Why two stores?** The extension store acts as a cache for tool responses. The node store is the canonical source driven by libp2p. The extension store is populated from node events, creating eventual consistency.

**Broadcast history:**
- Capped at `MAX_BROADCAST_HISTORY = 200` entries (M3 fix)
- Oldest entries evicted when cap is exceeded
- Only populated by outgoing broadcasts from `mesh_broadcast` tool; incoming broadcasts are forwarded to the LLM but not stored

---

## Design Decisions & Fixes

These fixes were identified in the [OPTIMIZATION-REPORT.md](./OPTIMIZATION-REPORT.md) and subsequently implemented.

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

**Environment variables:**
- `PI_MESH_NAME` — agent name (overrides default, but CLI flag takes priority)
- `PI_COMM_NAME` — backward-compatible alias for agent name

**Config priority:** CLI flag → `PI_MESH_NAME` env var → `PI_COMM_NAME` env var → `pi-<hostname>`

---

## Commands

| Command | Role | Description |
|---|---|---|
| `/auto-reply [on\|off]` | Toggle | When on, all incoming mesh messages auto-echo without involving the LLM. No argument toggles current state. |
| `/mesh-list-peers` | Read | List all known peers with connection status, agent name, age |
| `/mesh-discover` | Read | Scan for recently discovered peers |
| `/mesh-prune` | Write | Remove all disconnected/stale peers immediately |

---

## Test Suite

All tests located in the project root (`*.mjs`). Run with `npm test` (requires build) or individual `node test-*.mjs`.

| Suite | File | Count | Type | Network Required |
|---|---|---|---|---|
| Comprehensive | `test-network.mjs` | 25 tests | Integration | ✅ (needs peers) |
| Rigorous | `rigorous-concurrent-test.mjs` | 82 tests (10 phases) | Stress/load | ✅ (needs peers) |
| FIFO Queue | `test-fifo-queue.mjs` | 8 tests | Unit | ❌ |
| Memory/Leak | `test-leak.mjs` | 7 tests | Unit | ❌ |
| Stable Identity | `test-identity.mjs` | 9 tests | Unit | ❌ |
| Negative/Edge | `test-negative.mjs` | 11 tests | Fuzzing | ✅ (needs peers) |
| Extension Import | `test-extension.mjs` | 1 test | Smoke | ❌ |

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

**Build:** TypeScript compiled to `dist/` (ES2022, NodeNext module resolution, strict mode).

---

## Future Considerations

1. **NAT traversal** — The `announceAddresses` config option exists but is not exercised. Relay-based NAT traversal or hole-punching could extend the mesh beyond the local network.

2. **Peer scoring / reputation** — The current mesh treats all peers equally. A scoring mechanism (e.g., based on response latency, reliability) could inform routing decisions.

3. **Protocol versioning** — The protocol is hardcoded as `/pi-agent/0.1.0`. A version negotiation mechanism would enable forward/backward compatibility.
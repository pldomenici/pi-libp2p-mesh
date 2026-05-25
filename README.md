# pi-libp2p-mesh

> **P2P mesh network extension for [pi agents](https://github.com/earendil-works/pi-coding-agent) — peer discovery, direct messaging, gossip broadcast, and persistent agent memory.**  
> Built on [libp2p](https://libp2p.io/), the modular peer-to-peer networking stack.

[![npm version](https://img.shields.io/npm/v/pi-libp2p-mesh)](https://www.npmjs.com/package/pi-libp2p-mesh)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

---

> **📖 Full architecture document:** [`ARCHITECTURE.md`](./ARCHITECTURE.md) — module breakdowns, data flow diagrams, design decisions, and test suite details.

---

## Features

- 🔍 **Automatic peer discovery** via mDNS (local network) and optional Kademlia DHT (wide-area)
- 💬 **Direct messaging** between pi agents over libp2p streams (`/pi-agent/0.1.0` protocol)
- 📢 **GossipSub broadcast** for group announcements and coordination
- 🔐 **End-to-end encryption** via Noise protocol
- 🧹 **Automatic stale peer pruning** — old/restarted agents are cleaned up
- 🧠 **Persistent agent memory** via ChromaDB — vector-backed storage with semantic recall across sessions. One ChromaDB instance serves the whole mesh.
- 🔌 **Auto-start ChromaDB** — the first node automatically starts and hosts ChromaDB; all other nodes discover and connect to it via the mesh.
- 🔧 **Ten LLM-callable tools:**
  | Tool | Description |
  |---|---|
  | `mesh_list_peers` | List all known peers |
  | `mesh_send` | Send a direct message to a peer |
  | `mesh_broadcast` | Broadcast to all peers |
  | `mesh_discover` | Scan for new peers |
  | `mesh_prune` | Remove stale/disconnected peers |
  | `memory_store` | Save a key-value memory entry for a peer |
  | `memory_recall` | Recall memories for a peer by key |
  | `memory_search` | Semantic search across stored memories |
  | `memory_keys` | List memory keys for a peer |
  | `memory_reconnect` | Reconnect to ChromaDB after restarts |

---

## Installation

```bash
npm install pi-libp2p-mesh
```

Then add the extension to your pi configuration (e.g., `~/.pi/config.json`):

```json
{
  "extensions": ["pi-libp2p-mesh"]
}
```

Or install it as a pi package:

```bash
pi install pi-libp2p-mesh
```

---

## Quick Start

Once installed, the mesh starts automatically on `session_start`. You'll see:

```
[pi-libp2p-mesh] info: Mesh node started as "pi-myhost" (12D3KooW...)
```

Use the built-in tools from your pi agent prompts:

### List peers
```
mesh_list_peers
```

### Send a direct message
```
mesh_send peerId="12D3KooW..." message="Hello from another agent!"
```

### Broadcast to all peers
```
mesh_broadcast message="Attention everyone!" type="announce"
```

### Discover new peers
```
mesh_discover
```

### Prune stale peers
```
mesh_prune
```

### Save a memory
```
memory_store peerId="12D3KooW..." key="prefs" value="Prefers concise answers"
```

### Recall memories
```
memory_recall peerId="12D3KooW..." key="prefs"
```

### Semantic search across all memories
```
memory_search query="what do peers prefer?"
```

---

## CLI Flags

| Flag | Type | Default | Description |
|---|---|---|---|
| `--agent-name` | string | `pi-<hostname>` | Agent name for the mesh (also from `PI_MESH_NAME` env var) |
| `--mesh-enable-dht` | boolean | `false` | Enable Kademlia DHT for wide-area peer discovery |
| `--mesh-gossip-topic` | string | `pi-broadcast` | GossipSub topic for broadcast messages |
| `--mesh-swarm-key` | string | — | Path to a `swarm.key` file for private P2P network (PSK) |
| `--mesh-chroma-host` | string | `localhost` | ChromaDB server hostname (or `CHROMA_HOST` env var) |
| `--mesh-chroma-port` | number | `8000` | ChromaDB server port (or `CHROMA_PORT` env var) |
| `--mesh-chroma-token` | string | — | Auth token for ChromaDB (`x-chroma-token` header) |
| `--mesh-chroma-data-path` | string | `~/.local/share/chroma` | ChromaDB data directory for persistence (or `CHROMA_DATA_PATH` env var) |
| `--mesh-memory-preset` | string | `large` | Memory limit preset: `small` (32K), `medium` (128K), `large` (1M) |
| `--mesh-memory-max-entries` | number | — | Override hard max entries returned by `memory_recall` |
| `--mesh-memory-truncate` | number | — | Override value truncation in chars for memory entries |
| `--mesh-memory-budget` | number | — | Override auto-retrieve context budget in chars |
| `--mesh-memory-exchange-truncate` | number | — | Override exchange truncation in chars |
| `--mesh-memory-distance` | float | — | Override distance threshold for search filtering (default 0.6) |

Example:

```bash
pi --agent-name my-agent --mesh-enable-dht --mesh-memory-preset medium
```

---

## Environment Variables

| Variable | Description |
|---|---|
| `PI_MESH_NAME` | Agent name (overrides `--agent-name` flag) |
| `PI_COMM_NAME` | Backward-compatible alias for agent name |
| `PI_SWARM_KEY` | Path to a `swarm.key` file (alternative to `--mesh-swarm-key`) |
| `CHROMA_HOST` | ChromaDB host (alternative to `--mesh-chroma-host`) |
| `CHROMA_PORT` | ChromaDB port (alternative to `--mesh-chroma-port`) |
| `CHROMA_TOKEN` | ChromaDB auth token (alternative to `--mesh-chroma-token`) |
| `CHROMA_DATA_PATH` | ChromaDB data directory (alternative to `--mesh-chroma-data-path`) |
| `PI_MEMORY_PRESET` | Memory limit preset: `small`, `medium`, `large` |

**Config priority:** CLI flag → environment variable → default

---

## Memory Presets

All read-side memory limits can be set at once with `--mesh-memory-preset`:

| Preset | Target model | Truncation | Max entries | Context budget | Exchange truncation |
|---|---|---|---|---|---|
| `small` | 32K tokens | 2,000 chars | 20 | 12,000 chars | 2,000 chars |
| `medium` | 128K tokens | 5,000 chars | 30 | 25,000 chars | 3,000 chars |
| `large` (default) | 1M tokens | 10,000 chars | 50 | 50,000 chars | 5,000 chars |

Individual flags override specific preset values:

```bash
pi --mesh-memory-preset medium --mesh-memory-budget 50000
```

---

## Commands

| Command | Description |
|---|---|
| `/mesh-auto-reply [on\|off]` | Toggle auto-reply mode (when on, incoming messages echo without LLM) |
| `/mesh-list-peers` | List all known peers on the mesh |
| `/mesh-discover` | Scan for new peers on the network |
| `/mesh-prune` | Remove all disconnected/stale peers |

---

## Architecture

```
┌─────────────────── Mesh Network ────────────────────┐
│                                                     │
│  ┌── Agent A ─────────────────────────────────────┐ │
│  │  ┌──────────────┐  ┌────────┐  ┌─────────────┐ │ │
│  │  │  index.ts    │◄─┤tools.ts│  │ memory.ts   │ │ │
│  │  │  lifecycle,  │  │ 9 tools│  │ AgentMemory │ │ │
│  │  │  FIFO queue, │  └────────┘  │ client      │ │ │
│  │  │  host elect. │              └──────┬──────┘ │ │
│  │  └──────┬───────┘                     │        │ │
│  │         │               ┌─────────────┘        │ │
│  │  ┌──────▼────────┐      │                      │ │
│  │  │ MeshProtocols │      │                      │ │
│  │  │  · sendMessage│      │                      │ │
│  │  │  · raw topic  │      │                      │ │
│  │  └──────┬────────┘      │                      │ │
│  │  ┌──────▼────────┐      │                      │ │
│  │  │   MeshNode    │      │                      │ │
│  │  └───────────────┘      │                      │ │
│  └─────────────────────────┼──────────────────────┘ │
│                            ▼                        │
│  ┌──────────────── ChromaDB ──────────────────────┐ │
│  │  (one instance, first node hosts)              │ │
│  │  pi_memory_* collection — all agents share     │ │
│  └────────────────────────────────────────────────┘ │
│                                                     │
│  ┌── Agent B ─────────────────────────────────────┐ │
│  │  (same structure, connects to Agent A's        │ │
│  │   ChromaDB via mesh-discovered host)           │ │
│  └────────────────────────────────────────────────┘ │
│                                                     │
│              ┌──────────┴──────────┐                │
│              │   libp2p overlay    │                │
│              │  TCP + WebSocket    │                │
│              │  mDNS + DHT         │                │
│              └─────────────────────┘                │
└─────────────────────────────────────────────────────┘
```

### Module Structure

| File | Purpose |
|---|---|
| `src/types.ts` | Shared TypeScript types and interfaces |
| `src/node.ts` | `MeshNode` — libp2p node factory & lifecycle |
| `src/protocols.ts` | `MeshProtocols` — direct messaging & GossipSub |
| `src/tools.ts` | Tool registration (5 mesh + 4 memory) for pi's LLM integration |
| `src/memory.ts` | `AgentMemory` — ChromaDB-backed persistent memory, semantic search |
| `src/chroma-lifecycle.ts` | `ChromaDBLifecycle` — ChromaDB process management (start/stop/health) |
| `src/index.ts` | Extension entry point, lifecycle wiring, FIFO request queue, host election, memory hooks |

---

## Protocol Stack

| Layer | Library | Purpose |
|---|---|---|
| **Transport** | `@libp2p/tcp` + `@libp2p/websockets` | Connection establishment |
| **Encryption** | `@chainsafe/libp2p-noise` | End-to-end encryption |
| **Multiplexing** | `@chainsafe/libp2p-yamux` | Multiple streams per connection |
| **Discovery** | `@libp2p/mdns` (always on) | Local network peer discovery |
| | `@libp2p/kad-dht` (optional) | Wide-area Kademlia-based discovery |
| **Identity** | `@libp2p/identify` | Peer identity exchange (agent name, protocols) |
| **Pub/Sub** | `@chainsafe/libp2p-gossipsub` | Topic-based broadcast messaging |
| **Private Network** | `@libp2p/pnet` | Optional swarm key (PSK) for private P2P networks |
| **Custom** | `/pi-agent/0.1.0` | Direct agent-to-agent CBOR-encoded messaging |
| | `pi-memory-host` (GossipSub topic) | ChromaDB host discovery — first node announces, others connect |
| **Memory** | `chromadb` + `@chroma-core/default-embed` | Persistent agent memory, semantic search |

---

## Development

```bash
# Clone
git clone https://github.com/pldomenici/pi-libp2p-mesh.git
cd pi-libp2p-mesh

# Install dependencies
npm install

# Build TypeScript
npm run build

# Run the comprehensive test suite (requires peers on the network)
npm test

# Run the rigorous concurrent test suite
npm run test:rigorous

# FIFO queue unit tests (no network required)
npm run test:fifo

# Stable identity / private key tests (no network required)
npm run test:identity

# Memory leak / queue stress tests (no network required)
npm run test:leak

# Negative/edge-case input tests (requires peers)
npm run test:negative

# Run all tests (non-network + network)
npm run test:all

# Run extension import test
npm run test:extension

# Watch mode for development
npm run watch
```

### Test Results

| Suite | Tests | Result |
|---|---|---|
| Comprehensive (`test-network.mjs`) | 26 tests | ✅ All passing |
| Rigorous (`rigorous-concurrent-test.mjs`) | 82 tests (10 phases) | ✅ All passing |
| FIFO Queue (`test-fifo-queue.mjs`) | 8 tests | ✅ All passing |
| Stable Identity (`test-identity.mjs`) | 9 tests | ✅ All passing |
| Memory/Leak (`test-leak.mjs`) | 7 tests | ✅ All passing |
| Negative Input (`test-negative.mjs`) | 11 tests | ✅ All passing |

---

## Configuration

### Stable Peer Identity

Pass a persistent Ed25519 private key via `MeshConfig.privateKey` to give an agent a stable PeerId across restarts:

```ts
import { MeshNode } from 'pi-libp2p-mesh/node';

const node = await MeshNode.create({
  agentName: 'my-agent',
  privateKey: loadPersistedPrivateKey(), // Uint8Array
});
```

### Peer Persistence

The mesh maintains an in-memory peer store that persists for the duration of the session. Stale peers (disconnected > 60s) are automatically pruned every 30 seconds. Use `mesh_prune` for immediate cleanup.

### Memory Persistence

ChromaDB data is stored on disk at `.chroma/`. Memories persist across agent restarts — on next `session_start`, the extension reconnects to the existing collection and all past memories are immediately available.

---

## Further Reading

- [`ARCHITECTURE.md`](./ARCHITECTURE.md) — comprehensive module breakdowns, data flow diagrams, design decisions
- [`AGENT-MEMORY.md`](./AGENT-MEMORY.md) — LLM usage guide for the memory system

---

## License

MIT © [Earendil Works](LICENSE)

# pi-libp2p-mesh

> **P2P mesh network extension for [pi agents](https://github.com/earendil-works/pi-coding-agent) — peer discovery, direct messaging, and gossip broadcast.**  
> Built on [libp2p](https://libp2p.io/), the modular peer-to-peer networking stack.

[![npm version](https://img.shields.io/npm/v/pi-libp2p-mesh)](https://www.npmjs.com/package/pi-libp2p-mesh)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

---

## Features

- 🔍 **Automatic peer discovery** via mDNS (local network) and optional Kademlia DHT (wide-area)
- 💬 **Direct messaging** between pi agents over libp2p streams (`/pi-agent/0.1.0` protocol)
- 📢 **GossipSub broadcast** for group announcements and coordination
- 🔐 **End-to-end encryption** via Noise protocol
- 🧹 **Automatic stale peer pruning** — old/restarted agents are cleaned up
- 🔧 **Five custom tools** exposed to the LLM:
  - `mesh_list_peers` — list all known peers
  - `mesh_send` — send a direct message to a peer
  - `mesh_broadcast` — broadcast to all peers
  - `mesh_discover` — scan for new peers
  - `mesh_prune` — remove stale/disconnected peers

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

---

## CLI Flags

| Flag | Type | Default | Description |
|---|---|---|---|
| `--agent-name` | string | `pi-<hostname>` | Agent name for the mesh (also reads `PI_MESH_NAME` env var) |
| `--mesh-enable-dht` | boolean | `false` | Enable Kademlia DHT for wide-area peer discovery |
| `--mesh-gossip-topic` | string | `pi-broadcast` | GossipSub topic for broadcast messages |

Example:

```bash
pi --agent-name my-agent --mesh-enable-dht
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
┌─────────────────────────────────────────────────────┐
│                   pi agent                           │
│  ┌─────────────────────────────────────────────┐    │
│  │        pi-libp2p-mesh extension              │    │
│  │  ┌──────────┐  ┌────────────┐  ┌─────────┐  │    │
│  │  │  node.ts │  │protocols.ts│  │ tools.ts│  │    │
│  │  │ (libp2p) │  │ (msg/gossip)│  │ (tools)  │  │    │
│  │  └──────────┘  └────────────┘  └─────────┘  │    │
│  └─────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────┘
         │                │
    ┌────┴────┐    ┌──────┴──────┐
    │  mDNS   │    │  GossipSub  │
    │ discover│    │  broadcast  │
    └─────────┘    └─────────────┘
         │                │
    ┌────┴────────────────┴────────────────┐
    │         libp2p overlay network        │
    │  (TCP + WebSocket / Noise / Yamux)    │
    └───────────────────────────────────────┘
```

### Module Structure

| File | Purpose |
|---|---|
| `src/types.ts` | Shared TypeScript types and interfaces |
| `src/node.ts` | `MeshNode` — libp2p node factory & lifecycle |
| `src/protocols.ts` | `MeshProtocols` — direct messaging & GossipSub |
| `src/tools.ts` | Tool registration for pi's LLM integration |
| `src/index.ts` | Extension entry point, lifecycle wiring |

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
| Comprehensive (`test-network.mjs`) | 25 tests | ✅ All passing |
| Rigorous (`rigorous-concurrent-test.mjs`) | 82 tests (10 phases) | ✅ All passing |
| FIFO Queue (`test-fifo-queue.mjs`) | 8 tests | ✅ All passing |
| Stable Identity (`test-identity.mjs`) | 9 tests | ✅ All passing |
| Memory/Leak (`test-leak.mjs`) | 7 tests | ✅ All passing |
| Negative Input (`test-negative.mjs`) | 11 tests | ✅ All passing |

---

## Configuration

### Environment Variables

| Variable | Description |
|---|---|
| `PI_MESH_NAME` | Agent name (overrides `--agent-name` flag) |
| `PI_COMM_NAME` | Backward-compatible alias for agent name |

### Peer Persistence

The mesh maintains an in-memory peer store that persists for the duration of the session. Stale peers (disconnected > 60s) are automatically pruned every 30 seconds. Use `mesh_prune` for immediate cleanup.

### Stable Peer Identity

Pass a persistent Ed25519 private key via `MeshConfig.privateKey` to give an agent a stable PeerId across restarts:

```ts
import { MeshNode } from 'pi-libp2p-mesh/node';

const node = await MeshNode.create({
  agentName: 'my-agent',
  privateKey: loadPersistedPrivateKey(), // Uint8Array
});
```

---

## License

MIT © [Earendil Works](LICENSE)

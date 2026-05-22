# pi-libp2p-mesh — Distributed Code Optimization Report

**Date:** 2026-05-22  
**Organizer:** paul (mesh coordinator)  
**Reviewers:** bob (architecture), blair (protocols), paul (tools/lifecycle)  
**Codebase:** 5 files, 1,684 lines of TypeScript

---

## Methodology

Each peer was assigned a specific codebase layer for independent analysis:
- **bob** → `src/node.ts` + `src/types.ts` (architecture & node layer)
- **blair** → `src/protocols.ts` (protocol layer)
- **paul** → `src/tools.ts` + `src/index.ts` (tool layer & lifecycle)

All three peers read their assigned files, identified optimization opportunities, and returned structured reports with line references and impact ratings.

---

## Consolidated Findings (by severity)

### 🔴 HIGH — 4 findings

| # | Layer | Finding | Peer | Est. Impact |
|---|---|---|---|---|
| H1 | **node.ts** | Auto-dial storms under rapid mDNS discovery — `setTimeout` flood with no dedup | bob | Connection failures in churn |
| H2 | **node.ts** | `getPeers()` only returns connected peers — no internal peer store, stale entries accumulate | bob | Peer list divergence |
| H3 | **protocols.ts** | `readStream()` doesn't propagate AbortSignal — indefinite hang on unresponsive peers | blair | Resource exhaustion |
| H4 | **index.ts** | `onRequest` race — concurrent messages collide on `turn_end`, listener leak | paul | Incorrect routing, leaked listeners |

### 🟡 MEDIUM — 3 findings

| # | Layer | Finding | Peer | Est. Impact |
|---|---|---|---|---|
| M1 | **node.ts** | Ed25519 key generated on every `MeshNode.create()` — no persistence, identity churn | bob | Stale peer accumulation |
| M2 | **protocols.ts** | `handleIncomingMessage` silently swallows errors — sender waits full 30s timeout | blair | Latency amplification |
| M3 | **tools.ts** | `broadcastHistory` array grows unbounded — no eviction policy | paul | Memory leak |

### 🟢 LOW — 2 findings

| # | Layer | Finding | Peer | Est. Impact |
|---|---|---|---|---|
| L1 | **protocols.ts** | Double-pass memory copy in `readStream` — 2× overhead for large payloads | blair | GC pressure |
| L2 | **tools.ts** | Redundant peer map iteration — `listPeers` + `pruneStalePeers` + `pruneAllDisconnected` do 5 passes | paul | Unnecessary overhead |

---

## Detailed Findings

### 🔴 H1: Auto-dial storms (node.ts:179-187)

**Observation by bob:** `_onPeerDiscovery` fires a `setTimeout(() => this.libp2p.dial(peerId), 500)` for every discovered peer. No deduplication means re-discovered peers trigger new dials. No batching means N simultaneous dials for N newly discovered peers.

**Witnessed in testing:** When 5 `rigorous-test-*` instances joined simultaneously, the auto-dial storm could overwhelm the libp2p dialer queue. The `console.debug` on line 185 silently masks dial failures.

**Recommendation:** Add a `Set<string>` tracking pending dials. Debounce with a 200ms coalescing window — batch newly discovered peers and dial them in one microtask.

---

### 🔴 H2: No internal peer store (node.ts:117-132)

**Observation by bob:** `getPeers()` queries `libp2p.getPeers()` which only returns *currently connected* peers. Disconnected peers (🔴 in the UI) are invisible at the node layer. The `MeshPeer` interface already defines `status: "disconnected"` and `disconnectedAt` fields, but they're never populated because `getPeers()` never returns disconnected peers.

**Witnessed in testing:** The rigorous test runners disconnected but left 🔴 entries that couldn't be cleaned up at the node level. The tool layer (`mesh_list_peers`) maintains its own separate peer store in `tools.ts`, which diverges from the node's actual state.

**Recommendation:** Add an internal `Map<string, MeshPeer>` that persists peers across connect/disconnect cycles. On `peer:disconnect`, mark as disconnected and set `disconnectedAt`. Have `getPeers()` query this store.

---

### 🔴 H3: Timeout not propagated (protocols.ts:152-160)

**Observation by blair:** `sendMessage()` creates an `AbortController` with a 30s timeout. The signal is passed to `dialProtocol`, `stream.sink`, and `stream.closeWrite`, but **not** to `readStream()`. If the remote peer closes write but never sends a response, `readStream` hangs indefinitely in its `for await (const chunk of stream.source)` loop.

**Recommendation:** Either pass `abortController.signal` into `readStream` and pipe through to the stream reader, or wrap the stream source with a signal-checking async iterator.

---

### 🔴 H4: Concurrent onRequest race (index.ts:140-175)

**Observation by paul:** The `onRequest` handler registers a `pi.on("turn_end", ...)` listener for each incoming message. If two messages arrive before a `turn_end` fires, both register listeners and both call `pi.sendUserMessage()`. Only the first listener captures the response — the second sees `settled = true` and returns early, but its `sendUserMessage` already submitted a stray message to the LLM. The second request's Promise hangs until the 60s timeout.

Additionally, listeners are never explicitly removed via `pi.off()`, causing a listener leak over time.

**Witnessed in testing:** Some concurrent prompt messages returned `[timeout]` — this is the root cause.

**Recommendation:** Maintain a FIFO request queue. Process one message per `turn_end`, dequeue the next, and clean up the listener after each turn.

---

### 🟡 M1: No key persistence (node.ts:60-61)

**Observation by bob:** `generateKeyPair("Ed25519")` is called on every `MeshNode.create()`. Each restart produces a new PeerId, making agents appear as strangers to previously connected peers. Old entries accumulate as 🔴 stale peers.

**Witnessed in testing:** Every `rigorous-test-*` instance got a fresh PeerId, and all previous instances persisted as disconnected entries.

**Recommendation:** Accept an optional `privateKey` in `MeshConfig`. The caller can persist the key to disk between restarts, giving agents stable identities.

---

### 🟡 M2: Silent error swallowing (protocols.ts:235-238)

**Observation by blair:** When `handleIncomingMessage` encounters an error (malformed JSON, handler throw), the catch block logs and re-throws nothing — no error response is written to the stream. The sender waits the full 30s timeout before learning of the failure.

**Recommendation:** In the catch block, write an error `AgentResponse` with `error: true` so the sender fails fast (<100ms).

---

### 🟡 M3: Unbounded broadcastHistory (tools.ts)

**Observation by paul:** `store.broadcastHistory` is a `BroadcastMessage[]` that grows without any cap or eviction. Every broadcast pushes an entry. Under sustained activity, this leaks memory.

**Recommendation:** Cap at 100-200 entries (ring buffer or shift when exceeded).

---

### 🟢 L1: Double-pass memory copy (protocols.ts:32-47)

**Observation by blair:** `readStream()` does two full passes: collect chunks, then copy into a contiguous buffer. For large payloads (100KB+), this doubles memory usage.

**Recommendation:** Use geometric buffer growth (double capacity) or pre-allocate from a content-length hint.

---

### 🟢 L2: Redundant peer iteration (tools.ts)

**Observation by paul:** A `/mesh-prune` command triggers `listPeers()` (calls `pruneStalePeers` → 2 iterations), then `pruneAllDisconnected()` (1 iteration), then `listPeers()` again (another 2 iterations) = 5 total passes.

**Recommendation:** Merge into a single pass that prunes by name, then by time, and returns counts.

---

## Health Scores by Layer

| Layer | Peer | Files | Score | Critical Issue |
|---|---|---|---|---|
| **Architecture & Node** | bob | node.ts, types.ts | **6/10** | No peer store → stale accumulation |
| **Protocol Layer** | blair | protocols.ts | **7/10** | Timeout not propagated → hangs |
| **Tool Layer & Lifecycle** | paul | tools.ts, index.ts | **7/10** | onRequest race → lost messages |
| **Overall** | — | All 5 files | **7/10** | 4 high-priority items to fix |

---

## Recommendation Priority

1. **Fix H4 first (index.ts onRequest race)** — fixes incorrect LLM routing under concurrency, the most user-visible bug
2. **Fix H3 (protocols.ts timeout)** — prevents indefinite hangs on unresponsive peers
3. **Fix H2 (node.ts peer store)** — eliminates stale peer accumulation at the root
4. **Fix H1 (node.ts auto-dial)** — prevents connection storms under churn
5. Then address M1-M3 and L1-L2 as time permits

---

*Report compiled by paul from distributed analyses by bob, blair, and paul over the P2P mesh network.*

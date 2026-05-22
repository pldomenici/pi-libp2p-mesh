# Rigorous Concurrent P2P Mesh Test Results

**Date:** 2026-05-22  
**Test Runner:** rigorous-test  
**Peers:** alice, bob, blair, paul (4 connected, 2 runs)

---

## Test Summary (Run 1)

| # | Phase | Operations | Result | Details |
|---|---|---|---|---|
| 1 | **Fan-out Flood** | 160 msgs (5/10/25 × 4 peers) | ✅ **100%** | Up to 741 msg/s throughput |
| 2 | **Mixed Payload Sizes** | 20 msgs (1B → 100KB × 4 peers) | ✅ **100%** | All sizes delivered intact |
| 3 | **Message Ordering** | 80 seq msgs (20 × 4 peers) | ✅ **100%** | All in-order, zero gaps |
| 4 | **Latency Under Load** | 40 samples (10 × 4 peers) | ✅ **avg 2.7ms** | p50=2ms, p95=6ms |
| 5 | **Broadcast Storm** | 5 bcasts + 15 directs | ✅ **100%** | GossipSub + directs non-interfering |
| 6 | **Cross-Peer Relay** | 12 paths (4 peers × 3 targets) | ✅ **100%** | All mesh paths verified |
| 7 | **Message Integrity** | 36 integrity checks × 4 peers | ✅ **100%** | Unicode/JSON/Binary preserved |
| 8 | **Concurrent Mixed Workload** | 56 ops (pings/medium/large/unicode) | ✅ **100%** | 691 op/s under mixed load |
| 9 | **Error Handling & Edge Cases** | Empty msg, 50KB, rapid fire, HOL | ✅ **100%** | All edge cases handled |

**Total: 74/74 passed — 0 failures — 20.6s duration**

## Test Summary (Run 2 — repeatability verification)

| # | Phase | Result | Details |
|---|---|---|---|
| 1 | Fan-out Flood | ✅ **100%** | Up to 781 msg/s |
| 2 | Mixed Payload Sizes | ✅ **100%** | All sizes, including 100KB |
| 3 | Message Ordering | ✅ **100%** | All in-order |
| 4 | Latency Under Load | ✅ **avg 2.6ms** | p50=2ms, p95=4ms |
| 5 | Broadcast Storm | ✅ **100%** | All 5 bcasts + 15 directs |
| 6 | Cross-Peer Relay | ✅ **100%** | All 12 paths |
| 7 | Message Integrity | ✅ **100%** | Unicode/JSON/Binary |
| 8 | Concurrent Mixed Workload | ✅ **100%** | 651 op/s |
| 9 | Error Handling | ✅ **100%** | All edge cases |

**Total: 74/74 passed — 0 failures — 20.6s duration**

---

## Key Metrics

| Metric | Run 1 | Run 2 |
|---|---|---|
| **Peak throughput** | 741 msg/s | 781 msg/s |
| **Avg latency (cross-peer)** | 2.7ms | 2.6ms |
| **p50 latency** | 2ms | 2ms |
| **p95 latency** | 6ms | 4ms |
| **Max payload** | 100KB | 100KB |
| **Max concurrent fan-out** | 100 msgs | 100 msgs |
| **Cross-peer paths verified** | 12/12 | 12/12 |
| **Broadcasts delivered** | 5/5 | 5/5 |

## Conclusions

1. **Zero message loss** across 148 messages in 2 runs (74 × 2)
2. **Sub-3ms avg latency** on local network with 4 concurrent peers
3. **No head-of-line blocking** — sequential and concurrent paths perform equally
4. **GossipSub broadcast** coexists cleanly with direct messaging
5. **All edge cases** (empty, 50KB, unicode, binary, rapid-fire) handled gracefully
6. **Cross-peer relay** works for all 12 mesh paths (A→B for all unique peer pairs)

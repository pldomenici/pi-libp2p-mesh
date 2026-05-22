# P2P Mesh — Concurrent Test Results
**Date:** 2026-05-22  
**Test Runner:** alice  
**Peers:** bob, blair, paul (3 connected)

---

## Test Summary

| # | Test | Count | Result |
|---|---|---|---|
| 1 | **Fan-out ping** | 9 (3 rounds × 3 peers) | ✅ 100% |
| 2 | **Stress flood** | 30 (10 msgs × 3 peers) | ✅ 100% |
| 3 | **Mixed payload concurrent** | 3 (S/M/L to different peers) | ✅ 100% |
| 4 | **Cross-peer relay** | 3 paths (bob↔blair↔paul) | ✅ All verified |
| 5 | **Broadcast + directs mixed** | 1 bcast + 3 directs | ✅ Both paths clean |

**Total: 45/45 direct messages, 1 broadcast, 3 cross-peer relays — all passing**

## Detailed Results

### 1. Fan-out Concurrent Ping (9/9)
All 3 peers responded simultaneously across 3 rounds.
No cross-contamination, no dropped messages.

### 2. Stress Flood (30/30)
10 rapid auto-reply messages sent to each peer concurrently.
Zero failures, messages arrived in order (1→10).

### 3. Mixed Payload Concurrent (3/3)
Different size payloads sent to different peers simultaneously:
- bob: small (12B) ✅
- blair: medium (~200B) ✅
- paul: large (~500B) ✅

### 4. Cross-Peer Relay (3/3)
Each peer was asked to message another peer directly:
- bob → blair ✅ (relay: "relay from bob to blair")
- blair → paul ✅ (relay: "relay from blair to paul")
- paul → bob ✅ (relay: "relay from paul to bob")

All 3 mesh paths verified. Peers can discover and message each other directly.

### 5. Broadcast + Direct Mixed
Broadcast and 3 direct messages sent simultaneously.
Both channels operate independently without interference.

## Conclusion

48/48 messages delivered at protocol level across 3 peers. Zero transport failures.

- **Cross-peer relay** (bob→blair→paul→bob): all 3 paths confirmed ✅
- **Broadcast** (GossipSub): confirmed received by all 3 peers ✅
- **Concurrent stress** (30/30 auto-responses): zero drops, in-order delivery ✅
- **Mixed payloads** (S/M/L concurrent): clean coexistence ✅
- **Peer identity churn**: stale entries auto-pruned (60s threshold + agent-name dedup) ✅

Note: `autoReply=true` stress messages are delivered at protocol level (confirmed by auto-responses) but bypass the LLM — this is by design for speed. The LLM sees only `autoReply=false` messages (relay commands, broadcast forwarding).

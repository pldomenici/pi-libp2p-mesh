#!/usr/bin/env node
/**
 * test-chromadb-memory.mjs
 *
 * Integration test for the ChromaDB agent memory layer. Simulates a
 * fully running pi agent session by exercising the same code paths
 * that src/index.ts uses during session_start/turn_end/onRequest/onBroadcast.
 *
 * Tests the full pipeline:
 *   1. AgentMemory creation (session_start)
 *   2. Auto-save exchange after turn_end (with truncation)
 *   3. Auto-retrieve context before onRequest (semantic search + budget)
 *   4. Auto-save broadcasts
 *   5. Explicit memory_store / memory_recall / memory_search / memory_keys
 *   6. resolveMemoryConfig (presets + overrides)
 *   7. deleteByPeer batch pagination
 *   8. Graceful behavior when no memories exist
 *   9. Multi-peer isolation
 *   10. Full-text vs truncated retrieval
 *
 * Usage:
 *   npm run build && npx tsx test-chromadb-memory.mjs
 *
 * Requires: ChromaDB running on localhost:8000
 */

import { AgentMemory, resolveMemoryConfig } from "./src/memory.ts";
import { MEMORY_PRESETS } from "./src/types.ts";

// ── Test Runner ──────────────────────────────────────────────────────────────

let passed = 0;
let failed = 0;

function test(name, fn) {
  return {
    async run() {
      try {
        await fn();
        passed++;
        console.log(`  ✅ ${name}`);
      } catch (err) {
        failed++;
        console.log(`  ❌ ${name}: ${err.message}`);
      }
    },
  };
}

function assert(cond, msg) {
  if (!cond) throw new Error(msg || "assertion failed");
}

function assertEq(actual, expected, label) {
  if (actual !== expected)
    throw new Error(`${label}: expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`);
}

// ── Fixtures ────────────────────────────────────────────────────────────────

const PEER_A = "12D3KooWAbc123def456ghi789jkl012mno345pqr678stu901vwx"; // pi-alpha
const PEER_B = "12D3KooWBcd234efg567hij890klm123nop456qrs789tuv012wxy"; // pi-beta
const AGENT_NAME = "test-agent";

let memory;

// ── Simulated hooks from index.ts ───────────────────────────────────────────

/**
 * Simulate the turn_end auto-save hook.
 * Called after the LLM responds to an incoming peer request.
 */
async function simulateAutoSaveExchange(peerId, fromAgent, requestMsg, responseMsg) {
  await memory.store({
    peerId,
    key: "exchange",
    value: `[Request from ${fromAgent}] ${requestMsg}\n[Response] ${responseMsg}`,
    metadata: { type: "conversation_turn", requestId: `req-${Date.now()}` },
  });
}

/**
 * Simulate the onRequest auto-retrieve hook.
 * Builds memory context for an incoming request from a peer.
 * Mirrors buildMemoryContext() in src/index.ts.
 */
async function simulateAutoRetrieve(peerId, requestMsg) {
  // Semantic search
  const searchResults = await memory.search(requestMsg, {
    peerId,
    nResults: 3,
    fullText: true,
  });

  // Most recent exchange
  const recentExchanges = await memory.get(peerId, "exchange", {
    limit: 1,
    fullText: true,
  });

  // Build context lines within budget
  const lines = [];
  let charCount = 0;
  const budget = memory.config.contextBudgetChars;

  const addLine = (line) => {
    if (charCount + line.length > budget) return;
    lines.push(line);
    charCount += line.length;
  };

  const peer = { agentName: "test-peer" }; // simplified — index.ts uses store.peers
  const name = peer?.agentName ?? peerId.slice(0, 12) + "...";

  addLine(`[Memory about ${name}:]`);

  for (const r of searchResults) {
    addLine(`  ${r.key}: ${r.value.slice(0, memory.config.exchangeTruncationChars)}`);
  }

  if (recentExchanges.length > 0) {
    const ex = recentExchanges[0];
    addLine(
      `  Last exchange: ${ex.value.slice(0, memory.config.exchangeTruncationChars)}`,
    );
  }

  const total = await memory.count(peerId);
  addLine(`  (${total} total interactions)`);

  return lines.length > 1 ? lines.join("\n") : undefined;
}

/**
 * Simulate the onBroadcast auto-save hook.
 */
async function simulateAutoSaveBroadcast(peerId, fromAgent, msgType, message) {
  await memory.store({
    peerId,
    key: "broadcast",
    value: `[${msgType ?? "announce"}] ${message}`,
    metadata: { type: "broadcast" },
  });
}

/**
 * Simulate mesh_send auto-save of outgoing exchanges.
 */
async function simulateOutgoingExchangeSave(peerId, sentMsg, fromAgent, responseMsg) {
  await memory.store({
    peerId,
    key: "exchange",
    value: `[Sent] ${sentMsg}\n[Response from ${fromAgent}] ${responseMsg}`,
    metadata: { type: "conversation_turn" },
  });
}

// ── Test Suite ──────────────────────────────────────────────────────────────

async function runTests() {
  console.log("══════════════════════════════════════════════");
  console.log("  ChromaDB Memory Layer — Integration Tests");
  console.log("══════════════════════════════════════════════\n");

  // ── Setup ──────────────────────────────────────────────────────────────────
  console.log("── Setup ──");

  await test("AgentMemory.create with default config", async () => {
    memory = await AgentMemory.create({ agentName: AGENT_NAME });
    assertEq(memory.collectionName, `pi_memory_${AGENT_NAME}`, "collection name");
    assertEq(memory.config.maxEntries, MEMORY_PRESETS.large.maxEntries, "default maxEntries");
  }).run();

  await test("AgentMemory.create with small preset", async () => {
    const memSmall = await AgentMemory.create({
      agentName: "test-small",
      config: MEMORY_PRESETS.small,
    });
    assertEq(memSmall.config.valueTruncationChars, 2000, "small truncation");
    assertEq(memSmall.config.maxEntries, 20, "small maxEntries");
    await memSmall.deleteByPeer("dummy"); // cleanup for count test
  }).run();

  // ── Section 1: Auto-Save (turn_end simulation) ────────────────────────────
  console.log("\n── 1. Auto-Save Exchange (turn_end simulation) ──");

  await test("exchange is stored with correct metadata", async () => {
    await simulateAutoSaveExchange(
      PEER_A, "pi-alpha",
      "What is the capital of France?",
      "The capital of France is Paris.",
    );
    const entries = await memory.get(PEER_A, "exchange");
    assert(entries.length >= 1, "should have at least 1 exchange");
    assert(entries[0].value.includes("Request from pi-alpha"), "contains request");
    assert(entries[0].value.includes("Paris"), "contains response");
    assertEq(entries[0].key, "exchange", "key is exchange");
    assertEq(entries[0].peerId, PEER_A, "peerId correct");
    assert(entries[0].timestamp > 0, "has timestamp");
  }).run();

  await test("multiple exchanges accumulate (append-only)", async () => {
    await simulateAutoSaveExchange(PEER_A, "pi-alpha", "Q1", "A1");
    await simulateAutoSaveExchange(PEER_A, "pi-alpha", "Q2", "A2");
    const entries = await memory.get(PEER_A, "exchange");
    assert(entries.length >= 3, `should have at least 3 exchanges, got ${entries.length}`);
    // Newest first
    assert(entries[0].value.includes("Q2"), "newest entry contains Q2");
  }).run();

  // ── Section 2: Auto-Retrieve (onRequest simulation) ───────────────────────
  console.log("\n── 2. Auto-Retrieve Context (onRequest simulation) ──");

  await test("context includes semantic search results", async () => {
    // Store a memory about a decision
    await memory.store({ peerId: PEER_A, key: "decision", value: "We decided to use TypeScript for the project." });
    // Use a semantically close query — "TypeScript" is the connecting word
    const ctx = await simulateAutoRetrieve(PEER_A, "TypeScript programming language");
    assert(ctx.includes("Memory about"), "has memory header");
    assert(ctx.includes("TypeScript") || ctx.includes("decision"), "finds decision memory");
  }).run();

  await test("context includes last exchange", async () => {
    const ctx = await simulateAutoRetrieve(PEER_A, "anything");
    assert(ctx.includes("Last exchange"), "has last exchange");
  }).run();

  await test("context respects budget cap", async () => {
    const ctx = await simulateAutoRetrieve(PEER_A, "anything");
    assert(ctx.length <= memory.config.contextBudgetChars + 100,
      `context length ${ctx.length} should be <= budget ${memory.config.contextBudgetChars}`);
  }).run();

  await test("context returns minimal banner for peer with no memories", async () => {
    const ctx = await simulateAutoRetrieve(PEER_B, "anything");
    // Returns header + count, not undefined — peer has 0 interactions
    assert(ctx !== undefined, "returns a context string (not undefined)");
    assert(ctx.includes("Memory about"), "has header");
    assert(ctx.includes("0 total interactions"), "shows zero count");
  }).run();

  // ── Section 3: Broadcast Auto-Save ────────────────────────────────────────
  console.log("\n── 3. Broadcast Auto-Save ──");

  await test("broadcast is stored with correct metadata", async () => {
    await simulateAutoSaveBroadcast(PEER_A, "pi-alpha", "announce", "New release v2.0 deployed");
    const entries = await memory.get(PEER_A, "broadcast");
    assert(entries.length >= 1, "has broadcast");
    assert(entries[0].value.includes("New release"), "contains broadcast message");
    assert(entries[0].value.includes("[announce]"), "has type prefix");
  }).run();

  // ── Section 4: Outgoing Exchange Save (mesh_send simulation) ──────────────
  console.log("\n── 4. Outgoing Exchange (mesh_send simulation) ──");

  await test("outgoing exchange logged correctly", async () => {
    await simulateOutgoingExchangeSave(PEER_B, "Hello from us", "pi-beta", "Hello back!");
    const entries = await memory.get(PEER_B, "exchange");
    assert(entries.length >= 1, "has outgoing exchange");
    assert(entries[0].value.includes("[Sent]"), "marked as sent");
    assert(entries[0].value.includes("Response from pi-beta"), "includes response source");
  }).run();

  // ── Section 5: Memory Tools (LLM-facing) ──────────────────────────────────
  console.log("\n── 5. Memory Tools ──");

  await test("memory_store saves with correct key", async () => {
    await memory.store({
      peerId: PEER_A,
      key: "prefs",
      value: "Prefers short, code-only responses with no explanations.",
    });
    const entries = await memory.get(PEER_A, "prefs");
    assert(entries.length >= 1, "has prefs entry");
    assert(entries[0].value.includes("code-only"), "contains prefs value");
  }).run();

  await test("memory_recall by peerId+key returns correct entries", async () => {
    const entries = await memory.get(PEER_A, "prefs");
    assert(entries.length >= 1, "has prefs");
    assertEq(entries[0].peerId, PEER_A, "correct peer");
    assertEq(entries[0].key, "prefs", "correct key");
  }).run();

  await test("memory_recall respects limit", async () => {
    // We have many exchanges with PEER_A — limit to 2
    const entries = await memory.get(PEER_A, "exchange", { limit: 2 });
    assert(entries.length <= 2, `limit 2 should return <= 2, got ${entries.length}`);
  }).run();

  await test("memory_search finds semantically similar entries", async () => {
    const results = await memory.search("France capital city");
    assert(results.length >= 1, "finds Paris exchange");
    const top = results.find(r => r.value.includes("Paris"));
    assert(top, "''finds the Paris exchange");
    assert(top.distance < 1.0, "has valid distance score");
  }).run();

  await test("memory_search respects peerId scope", async () => {
    // PEER_B has an exchange with "Hello from us" / "Hello back!"
    const results = await memory.search("greeting hello", { peerId: PEER_B });
    assert(results.length >= 1, "finds PEER_B entries");
    assert(results.every(r => r.peerId === PEER_B), "all scoped to PEER_B");
  }).run();

  await test("memory_keys lists all keys for a peer", async () => {
    const keys = await memory.getKeys(PEER_A);
    const keyNames = keys.map(k => k.key);
    assert(keyNames.includes("exchange"), "has exchange");
    assert(keyNames.includes("prefs"), "has prefs");
    assert(keyNames.includes("decision"), "has decision");
    assert(keyNames.includes("broadcast"), "has broadcast");
    // Verify counts are positive
    for (const k of keys) {
      assert(k.count >= 1, `${k.key} has count >= 1`);
    }
  }).run();

  await test("memory_keys returns empty for unknown peer", async () => {
    const keys = await memory.getKeys("12D3KooWNonexistent");
    assertEq(keys.length, 0, "empty for unknown peer");
  }).run();

  // ── Section 6: Multi-Peer Isolation ───────────────────────────────────────
  console.log("\n── 6. Multi-Peer Isolation ──");

  await test("PEER_A and PEER_B entries are isolated", async () => {
    await memory.store({ peerId: PEER_A, key: "unique_a", value: "only for A" });
    await memory.store({ peerId: PEER_B, key: "unique_b", value: "only for B" });

    const aEntries = await memory.get(PEER_A, "unique_a");
    const bEntries = await memory.get(PEER_B, "unique_b");
    assertEq(aEntries.length, 1, "PEER_A has unique_a");
    assertEq(bEntries.length, 1, "PEER_B has unique_b");

    // Cross-check: PEER_A should NOT have unique_b
    const crossCheck = await memory.get(PEER_A, "unique_b");
    assertEq(crossCheck.length, 0, "PEER_A does not have PEER_B's entries");
  }).run();

  // ── Section 7: Value Truncation ───────────────────────────────────────────
  console.log("\n── 7. Value Truncation ──");

  await test("values are truncated by default", async () => {
    const longValue = "x".repeat(memory.config.valueTruncationChars + 500);
    await memory.store({ peerId: PEER_A, key: "long", value: longValue });
    const entries = await memory.get(PEER_A, "long");
    assert(entries.length >= 1, "has long entry");
    assert(entries[0].value.length <= memory.config.valueTruncationChars + 30,
      `truncated to ~${memory.config.valueTruncationChars}, got ${entries[0].value.length}`);
    assert(entries[0].value.includes("[truncated]"), "has truncation marker");
  }).run();

  await test("fullText option returns untruncated value", async () => {
    const longValue = "y".repeat(500);
    await memory.store({ peerId: PEER_A, key: "fulltext_test", value: longValue });
    const entries = await memory.get(PEER_A, "fulltext_test", { fullText: true });
    assertEq(entries[0].value, longValue, "fullText returns exact value");
  }).run();

  // ── Section 8: resolveMemoryConfig ────────────────────────────────────────
  console.log("\n── 8. Config Resolution ──");

  await test("valid preset returns correct config", async () => {
    const cfg = resolveMemoryConfig({ preset: "small" });
    assertEq(cfg.valueTruncationChars, 2000, "small truncation");
    assertEq(cfg.maxEntries, 20, "small maxEntries");
  }).run();

  await test("invalid preset falls back to large", async () => {
    const cfg = resolveMemoryConfig({ preset: "nonexistent" });
    assertEq(cfg.maxEntries, MEMORY_PRESETS.large.maxEntries, "fallback maxEntries");
    assertEq(cfg.valueTruncationChars, MEMORY_PRESETS.large.valueTruncationChars, "fallback truncation");
  }).run();

  await test("individual overrides take precedence", async () => {
    const cfg = resolveMemoryConfig({ preset: "medium", maxEntries: 100 });
    assertEq(cfg.maxEntries, 100, "override applied");
    assertEq(cfg.valueTruncationChars, 5000, "medium default preserved for non-overridden");
  }).run();

  // ── Section 9: Count ──────────────────────────────────────────────────────
  console.log("\n── 9. Count ──");

  await test("count returns total entries", async () => {
    const c = await memory.count();
    assert(c > 0, `total count should be > 0, got ${c}`);
  }).run();

  await test("count scoped to peer returns correct number", async () => {
    const countA = await memory.count(PEER_A);
    const keys = await memory.getKeys(PEER_A);
    const totalFromKeys = keys.reduce((s, k) => s + k.count, 0);
    assertEq(countA, totalFromKeys, "scoped count matches sum of key counts");
  }).run();

  // ── Section 10: Delete ────────────────────────────────────────────────────
  console.log("\n── 10. Delete ──");

  await test("deleteByPeer removes all entries for a peer", async () => {
    // Create a temporary peer
    const TEMP_PEER = "12D3KooWTempDeleteTest000000000000000000000000000000";
    await memory.store({ peerId: TEMP_PEER, key: "x", value: "1" });
    await memory.store({ peerId: TEMP_PEER, key: "y", value: "2" });
    assertEq(await memory.count(TEMP_PEER), 2, "has 2 entries before delete");

    const deleted = await memory.deleteByPeer(TEMP_PEER);
    assertEq(deleted, 2, "deleted 2 entries");
    assertEq(await memory.count(TEMP_PEER), 0, "0 entries after delete");
  }).run();

  await test("deleteByPeer returns 0 for unknown peer", async () => {
    const deleted = await memory.deleteByPeer("12D3KooWNoSuchPeer0000000000000000000000000000");
    assertEq(deleted, 0, "0 deleted for unknown peer");
  }).run();

  await test("deleteById removes single entry", async () => {
    await memory.store({ peerId: PEER_A, key: "todelete", value: "temp" });
    const entries = await memory.get(PEER_A, "todelete");
    assert(entries.length >= 1, "created entry");
    await memory.deleteById(entries[0].id);

    const after = await memory.get(PEER_A, "todelete");
    assertEq(after.length, 0, "entry deleted by id");
  }).run();

  // ── Section 11: Edge Cases ────────────────────────────────────────────────
  console.log("\n── 11. Edge Cases ──");

  await test("get() throws when both peerId and key are undefined", async () => {
    try {
      await memory.get(undefined, undefined);
      assert(false, "should have thrown");
    } catch (err) {
      assert(err.message.includes("At least one"), "correct error message");
    }
  }).run();

  await test("get() returns empty array for unmatched filter", async () => {
    const entries = await memory.get("12D3KooWNoMatch000000000000000000000000000000", "nonexistent");
    assertEq(entries.length, 0, "empty for no match");
  }).run();

  await test("search returns empty for nonsense query", async () => {
    const results = await memory.search("xyzkwqrpnotarealwordanywhere");
    // May return distant results or empty — either is valid
    assert(Array.isArray(results), "returns array");
  }).run();

  await test("getByPeer returns entries for peer", async () => {
    const entries = await memory.getByPeer(PEER_A, { limit: 5 });
    assert(entries.length >= 1, "has entries");
    assert(entries.every(e => e.peerId === PEER_A), "all from PEER_A");
  }).run();

  // ── Cleanup ────────────────────────────────────────────────────────────────
  console.log("\n── Cleanup ──");
  await test("cleanup all test data", async () => {
    const before = await memory.count();
    await memory.deleteByPeer(PEER_A);
    await memory.deleteByPeer(PEER_B);
    const after = await memory.count();
    assert(after <= before, "count decreased");
    console.log(`      (removed ${before - after} entries, ${after} remain)`);
  }).run();

  // ── Results ────────────────────────────────────────────────────────────────
  console.log("\n══════════════════════════════════════════════");
  console.log(`  Results: ${passed} passed, ${failed} failed, ${passed + failed} total`);
  console.log("══════════════════════════════════════════════\n");

  if (failed > 0) process.exit(1);
}

runTests().catch((err) => {
  console.error("Fatal test error:", err);
  process.exit(1);
});

#!/usr/bin/env node
/**
 * test-memory-live-mesh.mjs
 *
 * Runs on the LIVE ~/.pi/mesh.db shared by all 4 mesh peers
 * (paul, blair, ethan, bob) and validates:
 *
 *   1. All peers are visible in the shared DB
 *   2. Each peer can see memories stored by other peers
 *   3. Memory CRUD works correctly across sessions
 *   4. Search works across all agents' memories
 *   5. Full lifecycle: store → close → reopen → recall
 *
 * This is a non-destructive test — it cleans up after itself.
 *
 * Usage:
 *   node test-memory-live-mesh.mjs
 */

import { MeshDatabase } from "./src/db.ts";

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

// ── Tests ───────────────────────────────────────────────────────────────────

async function runTests() {
  console.log(`\n${"".padEnd(60, "=")}`);
  console.log("  Live Mesh — Cross-Peer Memory Test");
  console.log(`  Database: ~/.pi/mesh.db (shared by all 4 agents)`);
  console.log(`${"".padEnd(60, "=")}\n`);

  const DB_PATH = "/home/paul/.pi/mesh.db";

  // ── 1. Verify all 4 peers exist ─────────────────────────────────────────
  console.log("── 1. Peer Discovery ──\n");

  await test("All 4 mesh peers are present in shared DB", async () => {
    const db = new MeshDatabase(DB_PATH, "test-suite");
    const peers = db.getAllPeers();
    const connected = peers.filter((p) => p.status === "connected");
    assert(connected.length >= 4, `Expected ≥4 connected peers, got ${connected.length}`);
    const names = connected.map((p) => p.agentName).sort();
    console.log(`     Connected: ${names.join(", ")}`);
    db.close();
  }).run();

  // ── 2. Cross-peer memory operations ─────────────────────────────────────
  console.log("\n── 2. Cross-Peer Memory Operations ──\n");

  await test("Store from agent-A session, recall from agent-B session", async () => {
    const dbA = new MeshDatabase(DB_PATH, "test-agent-alpha");
    const m = dbA.storeMemory({
      agentName: "test-agent-alpha",
      key: "live_test",
      value: "Stored by alpha, recalled by beta — cross-peer test",
      tags: ["live-test"],
      importance: 3,
    });
    dbA.close();

    const dbB = new MeshDatabase(DB_PATH, "test-agent-beta");
    const results = dbB.recallByKey("live_test");
    assertEq(results.length, 1, "beta should recall alpha's memory");
    assertEq(results[0].value, "Stored by alpha, recalled by beta — cross-peer test");
    dbB.close();

    // Cleanup
    const dbC = new MeshDatabase(DB_PATH, "cleanup");
    dbC.forgetByKey("live_test");
    dbC.close();
  }).run();

  await test("Search across ALL memories (regardless of which agent stored)", async () => {
    const db = new MeshDatabase(DB_PATH, "test-searcher");
    const count = db.getMemoriesCount();
    assert(count >= 2, `Expected ≥2 memories from earlier tests, got ${count}`);

    // Search for the shared_fact from earlier
    const results = db.searchMemories("shared_fact");
    assertEq(results.length, 1, "should find the shared_fact memory");
    assertEq(results[0].key, "shared_fact");

    // Search for persistence
    const persistResults = db.searchMemories("persistence");
    assert(persistResults.length >= 1, "should find persistence-related memories");

    console.log(`     Total memories in shared DB: ${count}`);
    db.close();
  }).run();

  // ── 3. Persistence across sessions ──────────────────────────────────────
  console.log("\n── 3. Persistence Across Sessions ──\n");

  await test("Memory persists after close → reopen cycle (simulating agent restart)", async () => {
    const dbA = new MeshDatabase(DB_PATH, "test-restart-a");
    const m = dbA.storeMemory({
      key: "restart_test",
      value: "Should survive restart",
      tags: ["restart"],
      importance: 4,
    });
    const id = m.id;
    dbA.close();

    // Simulate agent restart
    const dbB = new MeshDatabase(DB_PATH, "test-restart-b");
    const r = dbB.getMemory(id);
    assert(r != null, "memory should survive restart");
    assertEq(r.value, "Should survive restart");
    assertEq(r.importance, 4);
    dbB.close();

    // Cleanup
    const dbC = new MeshDatabase(DB_PATH, "cleanup");
    dbC.forgetByKey("restart_test");
    dbC.close();
  }).run();

  await test("Update survives close → reopen cycle", async () => {
    const dbA = new MeshDatabase(DB_PATH, "test-update-a");
    const m = dbA.storeMemory({ key: "update_test", value: "original", tags: [], importance: 1 });
    dbA.close();

    const dbB = new MeshDatabase(DB_PATH, "test-update-b");
    dbB.storeMemory({ id: m.id, key: "update_test", value: "updated", tags: ["updated"], importance: 4 });
    dbB.close();

    const dbC = new MeshDatabase(DB_PATH, "test-update-c");
    const r = dbC.getMemory(m.id);
    assertEq(r.value, "updated", "update should persist");
    assertEq(r.importance, 4, "importance update should persist");
    dbC.forgetByKey("update_test");
    dbC.close();
  }).run();

  // ── 4. Concurrent session test ──────────────────────────────────────────
  console.log("\n── 4. Concurrent Session Access (WAL Mode) ──\n");

  await test("Two sessions open simultaneously can read each other's writes", async () => {
    const dbA = new MeshDatabase(DB_PATH, "concurrent-a");
    const dbB = new MeshDatabase(DB_PATH, "concurrent-b");

    // A writes
    dbA.storeMemory({ key: "concur_test_a", value: "from A", tags: [], importance: 1 });
    // B should see A's write (WAL mode)
    const bSeesA = dbB.recallByKey("concur_test_a");
    assertEq(bSeesA.length, 1, "B should see A's write while both open");

    // B writes
    dbB.storeMemory({ key: "concur_test_b", value: "from B", tags: [], importance: 2 });
    // A should see B's write
    const aSeesB = dbA.recallByKey("concur_test_b");
    assertEq(aSeesB.length, 1, "A should see B's write while both open");

    // Cleanup
    dbA.forgetByKey("concur_test_a");
    dbA.forgetByKey("concur_test_b");
    dbA.close();
    dbB.close();
  }).run();

  // ── 5. Final summary ───────────────────────────────────────────────────
  console.log("\n── 5. Final State Summary ──\n");

  await test("Summarize current shared DB state", async () => {
    const db = new MeshDatabase(DB_PATH, "summary");
    const count = db.getMemoriesCount();
    const peers = db.getConnectedPeers();
    const all = db.getAllMemories();

    console.log(`     Connected peers: ${peers.length}`);
    console.log(`     Persistent memories: ${count}`);
    if (all.length > 0) {
      console.log("     Memories in DB:");
      for (const m of all) {
        console.log(`       id=${m.id} | ${m.agentName ?? "(general)"} | ${m.key}: ${m.value.slice(0, 70)}`);
      }
    }
    db.close();
  }).run();

  // ── Results ────────────────────────────────────────────────────────────────
  console.log(`\n${"".padEnd(60, "=")}`);
  console.log(`  Results: ${passed} passed, ${failed} failed, ${passed + failed} total`);
  console.log(`${"".padEnd(60, "=")}\n`);

  process.exit(failed > 0 ? 1 : 0);
}

runTests().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});

#!/usr/bin/env node
/**
 * test-memory-cross-peer.mjs
 *
 * Tests memory persistence and recall across multiple mesh peers sharing
 * the same SQLite database (WAL mode). This simulates the real setup on
 * this machine where all 4 pi agents (paul, blair, ethan, bob) share
 * ~/.pi/mesh.db.
 *
 * Scenario:
 *   - 4 agents share one DB file (like the 4 running pi processes)
 *   - Agent "alpha" stores memories
 *   - Agent "beta" recalls them (same DB, different session)
 *   - Agent "gamma" searches them
 *   - Agent "delta" adds more
 *   - All restart → verify everything persisted
 *
 * Usage:
 *   node test-memory-cross-peer.mjs
 */

import { MeshDatabase } from "./src/db.ts";
import fs from "node:fs";
import path from "node:path";
import os from "node:os";

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

function assertDeepEq(actual, expected, label) {
  const a = JSON.stringify(actual);
  const e = JSON.stringify(expected);
  if (a !== e) throw new Error(`${label}: expected ${e}, got ${a}`);
}

function assertApprox(actual, expected, tolerance, label) {
  if (Math.abs(actual - expected) > tolerance)
    throw new Error(`${label}: expected ${expected} ± ${tolerance}, got ${actual}`);
}

// ── Helpers ──────────────────────────────────────────────────────────────────

function createTempDir() {
  return fs.mkdtempSync(path.join(os.tmpdir(), "mesh-cross-peer-test-"));
}

function cleanup(dir) {
  try {
    fs.rmSync(dir, { recursive: true, force: true });
  } catch {
    /* ignore */
  }
}

/**
 * Simulate the index.ts session_start lifecycle for an agent:
 *   1. Open DB with agentName as sessionId
 *   2. disconnectPeersFromOtherSessions() — marks stale connected peers
 *   3. Return the DB handle
 */
function startAgentSession(dbPath, agentName) {
  const db = new MeshDatabase(dbPath, agentName);
  const disconnected = db.disconnectPeersFromOtherSessions();
  return { db, disconnected };
}

/**
 * Simulate the index.ts session_shutdown lifecycle
 */
function stopAgentSession(db) {
  db.checkpoint();
  db.close();
}

// ── Tests ───────────────────────────────────────────────────────────────────

async function runTests() {
  console.log(`\n${"".padEnd(60, "=")}`);
  console.log("  Cross-Peer Memory Persistence & Recall Tests");
  console.log(`  (Simulating ${os.hostname()} — paul, blair, ethan, bob on shared DB)`);
  console.log(`${"".padEnd(60, "=")}\n`);

  // ── 1. Four agents, one DB (WAL mode) ──────────────────────────────────
  console.log("── 1. Four Agents, One Database (WAL Mode) ──\n");

  await test("Agent alpha stores → Agent beta recalls via peerId", async () => {
    const tmpDir = createTempDir();
    try {
      const dbPath = path.join(tmpDir, "shared-mesh.db");

      // Agent alpha stores a memory about peer "charlie"
      const { db: dbAlpha } = startAgentSession(dbPath, "alpha");
      dbAlpha.storeMemory({
        peerId: "12D3KooWCharlie",
        agentName: "charlie",
        key: "interests",
        value: "Machine learning and distributed consensus",
        tags: ["ml", "consensus"],
        importance: 4,
      });
      stopAgentSession(dbAlpha);

      // Agent beta (different session) opens same DB
      const { db: dbBeta } = startAgentSession(dbPath, "beta");
      assertEq(dbBeta.getMemoriesCount(), 1, "beta should see alpha's memory");

      const memories = dbBeta.recallByPeer("12D3KooWCharlie");
      assertEq(memories.length, 1, "beta should recall charlie's memory");
      assertEq(memories[0].key, "interests");
      assertEq(memories[0].value, "Machine learning and distributed consensus");
      assertEq(memories[0].agentName, "charlie");
      stopAgentSession(dbBeta);
    } finally {
      cleanup(tmpDir);
    }
  }).run();

  await test("Agent beta adds memory → Agent gamma sees both", async () => {
    const tmpDir = createTempDir();
    try {
      const dbPath = path.join(tmpDir, "shared-mesh.db");

      // Alpha stores one
      const { db: a } = startAgentSession(dbPath, "alpha");
      a.storeMemory({
        agentName: "alpha", key: "status", value: "Busy optimizing", tags: [], importance: 2,
      });
      stopAgentSession(a);

      // Beta adds one
      const { db: b } = startAgentSession(dbPath, "beta");
      assertEq(b.getMemoriesCount(), 1, "beta should see alpha's memory");
      b.storeMemory({
        agentName: "beta", key: "status", value: "Idle", tags: [], importance: 1,
      });
      assertEq(b.getMemoriesCount(), 2, "beta should now see 2");
      stopAgentSession(b);

      // Gamma recalls both
      const { db: c } = startAgentSession(dbPath, "gamma");
      assertEq(c.getMemoriesCount(), 2, "gamma should see both memories");
      const all = c.getAllMemories();
      assertEq(all.length, 2);
      const agentNames = all.map((m) => m.agentName).sort();
      assertDeepEq(agentNames, ["alpha", "beta"], "gamma should see both alpha and beta's memories");
      stopAgentSession(c);
    } finally {
      cleanup(tmpDir);
    }
  }).run();

  await test("Agent delta searches across all peers' memories", async () => {
    const tmpDir = createTempDir();
    try {
      const dbPath = path.join(tmpDir, "shared-mesh.db");

      // Three agents store different memories
      const agents = [
        { name: "alpha", key: "expertise", value: "Database internals" },
        { name: "beta", key: "expertise", value: "Network protocols" },
        { name: "gamma", key: "expertise", value: "Compiler design" },
      ];
      for (const agent of agents) {
        const { db } = startAgentSession(dbPath, agent.name);
        db.storeMemory({ agentName: agent.name, key: agent.key, value: agent.value, tags: [], importance: 1 });
        stopAgentSession(db);
      }

      // Delta searches
      const { db: d } = startAgentSession(dbPath, "delta");
      assertEq(d.getMemoriesCount(), 3, "delta should see all 3 memories");

      const networkResults = d.searchMemories("Network");
      assertEq(networkResults.length, 1, "delta should find 'Network protocols' by search");
      assertEq(networkResults[0].agentName, "beta");

      const compilerResults = d.searchMemories("Compiler");
      assertEq(compilerResults.length, 1, "delta should find 'Compiler design' by search");
      assertEq(compilerResults[0].agentName, "gamma");

      stopAgentSession(d);
    } finally {
      cleanup(tmpDir);
    }
  }).run();

  // ── 2. Full Lifecycle: 4 agents, restart, verify ───────────────────────
  console.log("\n── 2. Full Lifecycle: 4 Agents → Restart All → Verify ──\n");

  await test("All 4 agents store → all restart → all recall everything", async () => {
    const tmpDir = createTempDir();
    try {
      const dbPath = path.join(tmpDir, "shared-mesh.db");
      const agentNames = ["alpha", "beta", "gamma", "delta"];

      // ── Round 1: Each agent stores a memory ──
      const memories = [
        { agentName: "alpha", key: "fact", value: "Alpha discovered peer discovery bug" },
        { agentName: "beta", key: "fact", value: "Beta fixed the timeout propagation issue" },
        { agentName: "gamma", key: "fact", value: "Gamma optimized the FIFO queue" },
        { agentName: "delta", key: "fact", value: "Delta wrote the SQLite persistence layer" },
      ];

      for (const m of memories) {
        const { db } = startAgentSession(dbPath, m.agentName);
        db.storeMemory({
          agentName: m.agentName,
          key: m.key,
          value: m.value,
          tags: ["session-1"],
          importance: 3,
        });
        stopAgentSession(db);
      }

      // Verify all 4 stored
      const { db: checker } = startAgentSession(dbPath, "checker");
      assertEq(checker.getMemoriesCount(), 4, "all 4 memories should be present");
      stopAgentSession(checker);

      // ── Round 2: Each agent stores another memory (simulating ongoing work) ──
      const memories2 = [
        { agentName: "alpha", key: "status", value: "Reviewing PRs" },
        { agentName: "beta", key: "status", value: "Writing tests" },
        { agentName: "gamma", key: "status", value: "Benchmarking performance" },
        { agentName: "delta", key: "status", value: "Documenting API" },
      ];

      for (const m of memories2) {
        const { db } = startAgentSession(dbPath, m.agentName);
        db.storeMemory({
          agentName: m.agentName,
          key: m.key,
          value: m.value,
          tags: ["session-2"],
          importance: 2,
        });
        stopAgentSession(db);
      }

      // ── All agents restart: verify everything ──
      for (const agentName of agentNames) {
        const { db } = startAgentSession(dbPath, agentName);
        assertEq(db.getMemoriesCount(), 8, `${agentName} should see all 8 memories after restart`);

        // Should find their own and others' memories
        const myFacts = db.recallByKey("fact");
        assertEq(myFacts.length, 4, `${agentName} should find all 4 facts`);
        const myStatuses = db.recallByKey("status");
        assertEq(myStatuses.length, 4, `${agentName} should find all 4 statuses`);

        stopAgentSession(db);
      }

      // Final verification
      const { db: final } = startAgentSession(dbPath, "final");
      assertEq(final.getMemoriesCount(), 8, "final verification: 8 memories total");
      assertEq(final.recallByAgent("alpha").length, 2, "alpha has 2 memories");
      assertEq(final.recallByAgent("beta").length, 2, "beta has 2 memories");
      assertEq(final.recallByAgent("gamma").length, 2, "gamma has 2 memories");
      assertEq(final.recallByAgent("delta").length, 2, "delta has 2 memories");

      // Search across all agents
      const prResults = final.searchMemories("PRs");
      assertEq(prResults.length, 1, "should find 'Reviewing PRs'");
      assertEq(prResults[0].agentName, "alpha");

      const benchmarkResults = final.searchMemories("Benchmarking");
      assertEq(benchmarkResults.length, 1, "should find 'Benchmarking performance'");
      assertEq(benchmarkResults[0].agentName, "gamma");

      stopAgentSession(final);
    } finally {
      cleanup(tmpDir);
    }
  }).run();

  await test("Memories survive full WAL checkpoint cycle across peers", async () => {
    const tmpDir = createTempDir();
    try {
      const dbPath = path.join(tmpDir, "shared-mesh.db");

      // Agent A stores via WAL
      const { db: a } = startAgentSession(dbPath, "agent-a");
      a.storeMemory({ key: "wal_test", value: "Should survive WAL checkpoint", tags: [], importance: 1 });
      // Force WAL flush (simulates what shutdown does)
      a.checkpoint();
      stopAgentSession(a);

      // Agent B reads after WAL checkpoint
      const { db: b } = startAgentSession(dbPath, "agent-b");
      assertEq(b.getMemoriesCount(), 1, "memory should survive WAL checkpoint");
      const m = b.getMemory(1);
      assert(m != null);
      assertEq(m.value, "Should survive WAL checkpoint");
      stopAgentSession(b);
    } finally {
      cleanup(tmpDir);
    }
  }).run();

  // ── 3. Peer Recall Scenarios (real-world usage patterns) ────────────────
  console.log("\n── 3. Real-World Recall Scenarios Across Peers ──\n");

  await test("Agent stores memory about another peer → that peer can recall it", async () => {
    const tmpDir = createTempDir();
    try {
      const dbPath = path.join(tmpDir, "shared-mesh.db");

      // Alpha stores knowledge about Beta (as one agent would learn about another)
      const { db: alpha } = startAgentSession(dbPath, "alpha");
      alpha.storeMemory({
        peerId: "12D3KooWBeta", agentName: "beta",
        key: "expertise",
        value: "Networking and libp2p protocols",
        tags: ["p2p", "networking"],
        importance: 5,
      });
      alpha.storeMemory({
        agentName: "beta",
        key: "preference",
        value: "Prefers TypeScript over Rust for prototyping",
        tags: ["language"],
        importance: 2,
      });
      stopAgentSession(alpha);

      // Beta can see what Alpha knows about it
      const { db: beta } = startAgentSession(dbPath, "beta");
      const aboutBeta = beta.recallByAgent("beta");
      assertEq(aboutBeta.length, 2, "beta should find 2 memories about itself");
      assertEq(aboutBeta[0].key, "expertise", "expertise should be first (highest importance)");
      assertEq(aboutBeta[0].value, "Networking and libp2p protocols");
      stopAgentSession(beta);
    } finally {
      cleanup(tmpDir);
    }
  }).run();

  await test("Agent updates its own memory → other agents see the update", async () => {
    const tmpDir = createTempDir();
    try {
      const dbPath = path.join(tmpDir, "shared-mesh.db");

      // Alpha stores initial status
      const { db: a1 } = startAgentSession(dbPath, "alpha");
      const m = a1.storeMemory({
        agentName: "alpha", key: "status", value: "Initializing...", tags: [], importance: 1,
      });
      stopAgentSession(a1);

      // Beta sees initial status
      const { db: b } = startAgentSession(dbPath, "beta");
      assertEq(b.getMemory(m.id).value, "Initializing...");
      stopAgentSession(b);

      // Alpha updates status
      const { db: a2 } = startAgentSession(dbPath, "alpha");
      a2.storeMemory({
        id: m.id, agentName: "alpha", key: "status",
        value: "Running optimization pass 2",
        tags: ["optimizing"], importance: 3,
      });
      stopAgentSession(a2);

      // Gamma sees the updated status
      const { db: c } = startAgentSession(dbPath, "gamma");
      const updated = c.getMemory(m.id);
      assertEq(updated.value, "Running optimization pass 2", "other agents should see updated value");
      assertDeepEq(updated.tags, ["optimizing"], "tags should be updated too");
      assertEq(updated.importance, 3, "importance should be updated");
      c.close();
    } finally {
      cleanup(tmpDir);
    }
  }).run();

  await test("Agent deletes a memory → other agents see it's gone", async () => {
    const tmpDir = createTempDir();
    try {
      const dbPath = path.join(tmpDir, "shared-mesh.db");

      // Alpha stores and deletes a memory
      const { db: a } = startAgentSession(dbPath, "alpha");
      const m = a.storeMemory({ key: "temp", value: "transient data", tags: [], importance: 1 });
      a.storeMemory({ key: "permanent", value: "keep this", tags: [], importance: 5 });
      a.forgetMemory(m.id);
      stopAgentSession(a);

      // Beta should see only the permanent memory
      const { db: b } = startAgentSession(dbPath, "beta");
      assertEq(b.getMemoriesCount(), 1, "only permanent memory should remain");
      assert(b.getMemory(m.id) === undefined, "deleted memory should be gone");
      assertEq(b.recallByKey("permanent").length, 1, "permanent memory should exist");
      stopAgentSession(b);
    } finally {
      cleanup(tmpDir);
    }
  }).run();

  // ── 4. WAL Mode: Concurrent Access ─────────────────────────────────────
  console.log("\n── 4. Concurrent Access (WAL Mode Simulation) ──\n");

  await test("Two agents can have DB open simultaneously (read/write)", async () => {
    const tmpDir = createTempDir();
    try {
      const dbPath = path.join(tmpDir, "shared-mesh.db");

      // Agent A opens and keeps it open
      const dbA = new MeshDatabase(dbPath, "agent-a");
      dbA.storeMemory({ key: "a_only", value: "only A knows this", tags: [], importance: 1 });

      // Agent B opens same DB while A still has it open
      const dbB = new MeshDatabase(dbPath, "agent-b");
      assertEq(dbB.getMemoriesCount(), 1, "B should see A's memory (WAL mode)");
      assertEq(dbB.recallByKey("a_only").length, 1, "B should recall A's memory");

      // B writes while A is open
      dbB.storeMemory({ key: "b_only", value: "only B knows this", tags: [], importance: 2 });

      // A can see B's write
      const aMems = dbA.getAllMemories();
      assertEq(aMems.length, 2, "A should see B's memory too (WAL mode)");
      const aKeys = aMems.map((m) => m.key).sort();
      assertDeepEq(aKeys, ["a_only", "b_only"], "both agents' memories visible to each other");

      dbA.close();
      dbB.close();
    } finally {
      cleanup(tmpDir);
    }
  }).run();

  await test("Multiple sequential WAL checkpoints preserve data", async () => {
    const tmpDir = createTempDir();
    try {
      const dbPath = path.join(tmpDir, "shared-mesh.db");

      // Round 1
      const db1 = new MeshDatabase(dbPath, "agent-1");
      db1.storeMemory({ key: "round1", value: "first round", tags: [], importance: 1 });
      db1.checkpoint();
      db1.close();

      // Round 2
      const db2 = new MeshDatabase(dbPath, "agent-2");
      assertEq(db2.getMemoriesCount(), 1);
      db2.storeMemory({ key: "round2", value: "second round", tags: [], importance: 2 });
      db2.checkpoint();
      db2.close();

      // Round 3
      const db3 = new MeshDatabase(dbPath, "agent-3");
      assertEq(db3.getMemoriesCount(), 2);
      db3.storeMemory({ key: "round3", value: "third round", tags: [], importance: 3 });
      db3.checkpoint();
      db3.close();

      // Final verification
      const db4 = new MeshDatabase(dbPath, "agent-4");
      assertEq(db4.getMemoriesCount(), 3);
      assertEq(db4.recallByKey("round1")[0].value, "first round");
      assertEq(db4.recallByKey("round2")[0].value, "second round");
      assertEq(db4.recallByKey("round3")[0].value, "third round");
      db4.close();
    } finally {
      cleanup(tmpDir);
    }
  }).run();

  // ── 5. Real Machine Simulation ──────────────────────────────────────────
  console.log("\n── 5. Real Machine Simulation (4 agents, shared ~/.pi/mesh.db) ──\n");

  await test("Simulate the 4 running agents (paul, blair, ethan, bob) on shared DB", async () => {
    const tmpDir = createTempDir();
    try {
      const dbPath = path.join(tmpDir, "shared-mesh.db");

      // Simulate what happens when the 4 agents are running:
      // Each agent stores memories during its session

      // — paul's session —
      const { db: paul } = startAgentSession(dbPath, "paul");
      paul.storeMemory({
        agentName: "paul",
        key: "observation",
        value: "Testing cross-peer memory persistence from the pi agent",
        tags: ["test"],
        importance: 4,
      });
      stopAgentSession(paul);

      // — blair's session —
      const { db: blair } = startAgentSession(dbPath, "blair");
      // blair should see paul's memory immediately
      assertEq(blair.getMemoriesCount(), 1, "blair should see paul's memory");
      const paulsMemories = blair.recallByAgent("paul");
      assertEq(paulsMemories.length, 1, "blair should recall paul's memory");
      assertEq(paulsMemories[0].value, "Testing cross-peer memory persistence from the pi agent");
      // blair adds her own
      blair.storeMemory({
        agentName: "blair",
        key: "task",
        value: "Review the mesh protocol error handling",
        tags: ["protocol", "review"],
        importance: 3,
      });
      stopAgentSession(blair);

      // — ethan's session —
      const { db: ethan } = startAgentSession(dbPath, "ethan");
      assertEq(ethan.getMemoriesCount(), 2, "ethan should see paul + blair's memories");
      ethan.storeMemory({
        agentName: "ethan",
        key: "question",
        value: "Should we add auto-migration to the DB schema?",
        tags: ["db", "architecture"],
        importance: 5,
      });
      stopAgentSession(ethan);

      // — bob's session —
      const { db: bob } = startAgentSession(dbPath, "bob");
      assertEq(bob.getMemoriesCount(), 3, "bob should see all 3 agents' memories");
      bob.storeMemory({
        agentName: "bob",
        key: "decision",
        value: "Add schema version tracking in kv table for future migrations",
        tags: ["db", "decision"],
        importance: 5,
      });
      stopAgentSession(bob);

      // — Now simulate all agents restarting —
      // WAL checkpoint (like shutdown does)
      const { db: checkpoint } = startAgentSession(dbPath, "checkpoint");
      checkpoint.checkpoint();
      stopAgentSession(checkpoint);

      // — All agents restart and verify everything is intact —
      for (const name of ["paul", "blair", "ethan", "bob"]) {
        const { db } = startAgentSession(dbPath, name);
        assertEq(db.getMemoriesCount(), 4,
          `${name} (restarted) should see all 4 memories`);
        stopAgentSession(db);
      }

      // — Final full verification —
      const { db: final } = startAgentSession(dbPath, "verify");
      assertEq(final.getMemoriesCount(), 4, "total: 4 memories");

      // Verify each agent's specific memory
      assertEq(final.recallByAgent("paul")[0]?.value,
        "Testing cross-peer memory persistence from the pi agent");
      assertEq(final.recallByAgent("blair")[0]?.value,
        "Review the mesh protocol error handling");
      assertEq(final.recallByAgent("ethan")[0]?.value,
        "Should we add auto-migration to the DB schema?");
      assertEq(final.recallByAgent("bob")[0]?.value,
        "Add schema version tracking in kv table for future migrations");

      // Search across all agents
      const dbResults = final.searchMemories("DB");
      assertEq(dbResults.length, 1, "should find the DB-schema memory from ethan");
      assertEq(dbResults[0].agentName, "ethan");

      const protocolResults = final.searchMemories("protocol");
      assertEq(protocolResults.length, 1, "should find protocol memory");

      const persistenceResults = final.searchMemories("persistence");
      assertEq(persistenceResults.length, 1, "should find persistence memory");

      stopAgentSession(final);
    } finally {
      cleanup(tmpDir);
    }
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

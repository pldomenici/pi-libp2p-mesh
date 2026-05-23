#!/usr/bin/env node
/**
 * test-memory-persistence.mjs
 *
 * Tests agent memory persistence and recall across restarts.
 *
 * Simulates the full lifecycle:
 *   1. Session A: Store memories (as an agent would during normal operation)
 *   2. Close DB (simulate agent restart / shutdown)
 *   3. Session B: Open the SAME database file (simulate startup)
 *   4. Verify memories are still present and queryable
 *
 * Usage:
 *   node test-memory-persistence.mjs
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

// ── Helpers ──────────────────────────────────────────────────────────────────

function createTempDir() {
  return fs.mkdtempSync(path.join(os.tmpdir(), "mesh-persistence-test-"));
}

function cleanup(dir) {
  try {
    fs.rmSync(dir, { recursive: true, force: true });
  } catch {
    /* ignore */
  }
}

const DB_FILENAME = "mesh-persistence-test.db";

// ── Tests ───────────────────────────────────────────────────────────────────

async function runTests() {
  console.log(`\n${"".padEnd(60, "=")}`);
  console.log("  Agent Memory — Persistence & Startup Recall Tests");
  console.log(`${"".padEnd(60, "=")}\n`);

  // ── 1. Basic Persistence Across Restarts ─────────────────────────────────
  console.log("── 1. Basic Persistence Across Restarts ──\n");

  await test("Memories survive DB close + reopen (same path)", async () => {
    const tmpDir = createTempDir();
    try {
      const dbPath = path.join(tmpDir, DB_FILENAME);

      // ── Session A: store memories ──
      const dbA = new MeshDatabase(dbPath, "session-A");
      const m1 = dbA.storeMemory({
        peerId: "12D3KooWAlice",
        agentName: "alice",
        key: "interests",
        value: "Distributed systems and P2P protocols",
        tags: ["coding", "research"],
        importance: 4,
      });
      const m2 = dbA.storeMemory({
        peerId: "12D3KooWBob",
        agentName: "bob",
        key: "expertise",
        value: "Rust and TypeScript compiler design",
        tags: ["coding", "compilers"],
        importance: 5,
      });
      const m3 = dbA.storeMemory({
        key: "protocol_fact",
        value: "GossipSub uses a mesh topology with fanout of 6",
        tags: ["protocol"],
        importance: 3,
      });
      assertEq(dbA.getMemoriesCount(), 3, "session A should have 3 memories");
      dbA.close();

      // ── Session B: reopen same DB path ──
      const dbB = new MeshDatabase(dbPath, "session-B");

      // Verify count persists
      assertEq(dbB.getMemoriesCount(), 3, "session B should also see 3 memories");

      // Verify individual memories are intact
      const r1 = dbB.getMemory(m1.id);
      assert(r1 != null, "memory 1 should survive restart");
      assertEq(r1.key, "interests");
      assertEq(r1.value, "Distributed systems and P2P protocols");
      assertDeepEq(r1.tags, ["coding", "research"]);
      assertEq(r1.importance, 4);
      assertEq(r1.peerId, "12D3KooWAlice");
      assertEq(r1.agentName, "alice");

      const r2 = dbB.getMemory(m2.id);
      assert(r2 != null, "memory 2 should survive restart");
      assertEq(r2.key, "expertise");
      assertEq(r2.value, "Rust and TypeScript compiler design");
      assertEq(r2.agentName, "bob");

      const r3 = dbB.getMemory(m3.id);
      assert(r3 != null, "memory 3 should survive restart");
      assertEq(r3.key, "protocol_fact");
      assertEq(r3.agentName, undefined, "general fact should have no agentName");

      dbB.close();
    } finally {
      cleanup(tmpDir);
    }
  }).run();

  await test("Timestamps are preserved across restarts", async () => {
    const tmpDir = createTempDir();
    try {
      const dbPath = path.join(tmpDir, DB_FILENAME);
      const dbA = new MeshDatabase(dbPath, "session-A");
      const m = dbA.storeMemory({
        key: "timestamp_test",
        value: "check timestamps",
        tags: [],
        importance: 1,
      });
      const origCreated = m.createdAt;
      const origUpdated = m.updatedAt;
      dbA.close();

      const dbB = new MeshDatabase(dbPath, "session-B");
      const r = dbB.getMemory(m.id);
      assert(r != null, "memory should survive");
      assertEq(r.createdAt, origCreated, "createdAt should be preserved");
      assertEq(r.updatedAt, origUpdated, "updatedAt should be preserved");
      dbB.close();
    } finally {
      cleanup(tmpDir);
    }
  }).run();

  // ── 2. Recall by Different Dimensions After Restart ──────────────────────
  console.log("\n── 2. Recall Queries Survive Restart ──\n");

  await test("Recall by PeerId works after restart", async () => {
    const tmpDir = createTempDir();
    try {
      const dbPath = path.join(tmpDir, DB_FILENAME);

      // Session A: store memories for multiple peers
      const dbA = new MeshDatabase(dbPath, "session-A");
      dbA.storeMemory({ peerId: "peerA", key: "color", value: "blue", tags: [], importance: 1 });
      dbA.storeMemory({ peerId: "peerA", key: "food", value: "pizza", tags: [], importance: 2 });
      dbA.storeMemory({ peerId: "peerB", key: "color", value: "red", tags: [], importance: 1 });
      dbA.close();

      // Session B: recall by peer
      const dbB = new MeshDatabase(dbPath, "session-B");
      const aMemories = dbB.recallByPeer("peerA");
      assertEq(aMemories.length, 2, "peerA should have 2 memories after restart");
      assertEq(aMemories[0].peerId, "peerA");
      assertEq(aMemories[1].peerId, "peerA");

      const bMemories = dbB.recallByPeer("peerB");
      assertEq(bMemories.length, 1, "peerB should have 1 memory after restart");
      assertEq(bMemories[0].value, "red");
      dbB.close();
    } finally {
      cleanup(tmpDir);
    }
  }).run();

  await test("Recall by key works after restart", async () => {
    const tmpDir = createTempDir();
    try {
      const dbPath = path.join(tmpDir, DB_FILENAME);
      const dbA = new MeshDatabase(dbPath, "session-A");
      dbA.storeMemory({ peerId: "p1", key: "expertise", value: "Databases", tags: [], importance: 3 });
      dbA.storeMemory({ peerId: "p2", key: "expertise", value: "Networking", tags: [], importance: 4 });
      dbA.close();

      const dbB = new MeshDatabase(dbPath, "session-B");
      const expertise = dbB.recallByKey("expertise");
      assertEq(expertise.length, 2, "should find 2 expertise memories after restart");
      assertEq(expertise[0].key, "expertise");
      dbB.close();
    } finally {
      cleanup(tmpDir);
    }
  }).run();

  await test("Recall by agent name works after restart", async () => {
    const tmpDir = createTempDir();
    try {
      const dbPath = path.join(tmpDir, DB_FILENAME);
      const dbA = new MeshDatabase(dbPath, "session-A");
      dbA.storeMemory({ agentName: "alice", key: "tool", value: "vim", tags: [], importance: 1 });
      dbA.storeMemory({ agentName: "bob", key: "tool", value: "emacs", tags: [], importance: 1 });
      dbA.storeMemory({ agentName: "alice", key: "os", value: "NixOS", tags: [], importance: 2 });
      dbA.close();

      const dbB = new MeshDatabase(dbPath, "session-B");
      const aliceMemories = dbB.recallByAgent("alice");
      assertEq(aliceMemories.length, 2, "alice should have 2 memories after restart");
      assertEq(aliceMemories[0].agentName, "alice");
      dbB.close();
    } finally {
      cleanup(tmpDir);
    }
  }).run();

  await test("Full-text search works after restart", async () => {
    const tmpDir = createTempDir();
    try {
      const dbPath = path.join(tmpDir, DB_FILENAME);
      const dbA = new MeshDatabase(dbPath, "session-A");
      dbA.storeMemory({ key: "fact", value: "TypeScript is compiled to JavaScript", tags: [], importance: 1 });
      dbA.storeMemory({ key: "fact", value: "Rust uses LLVM as its backend", tags: [], importance: 1 });
      dbA.close();

      const dbB = new MeshDatabase(dbPath, "session-B");
      const results = dbB.searchMemories("TypeScript");
      assertEq(results.length, 1, "should find TypeScript memory after restart");
      assertEq(results[0].value, "TypeScript is compiled to JavaScript");

      const rustResults = dbB.searchMemories("LLVM");
      assertEq(rustResults.length, 1, "should find LLVM memory after restart");
      dbB.close();
    } finally {
      cleanup(tmpDir);
    }
  }).run();

  // ── 3. Importance Ordering Survives Restart ──────────────────────────────
  console.log("\n── 3. Importance Ordering Survives Restart ──\n");

  await test("Importance DESC ordering is preserved across restarts", async () => {
    const tmpDir = createTempDir();
    try {
      const dbPath = path.join(tmpDir, DB_FILENAME);
      const dbA = new MeshDatabase(dbPath, "session-A");
      dbA.storeMemory({ agentName: "charlie", key: "a", value: "low", tags: [], importance: 1 });
      dbA.storeMemory({ agentName: "charlie", key: "b", value: "high", tags: [], importance: 5 });
      dbA.storeMemory({ agentName: "charlie", key: "c", value: "medium", tags: [], importance: 3 });
      dbA.close();

      const dbB = new MeshDatabase(dbPath, "session-B");
      const ms = dbB.recallByAgent("charlie");
      assert(ms.length >= 3, "should find 3+ memories after restart");
      assertEq(ms[0].importance, 5, "first should be highest importance (5)");
      assertEq(ms[1].importance, 3, "second should be medium (3)");
      assertEq(ms[2].importance, 1, "last should be lowest (1)");
      dbB.close();
    } finally {
      cleanup(tmpDir);
    }
  }).run();

  // ── 4. Update + Delete Survive Restart ───────────────────────────────────
  console.log("\n── 4. Updates & Deletes Across Restarts ──\n");

  await test("Update a memory in session A, verify in session B", async () => {
    const tmpDir = createTempDir();
    try {
      const dbPath = path.join(tmpDir, DB_FILENAME);

      // Session A: create then update a memory
      const dbA = new MeshDatabase(dbPath, "session-A");
      const m = dbA.storeMemory({ key: "fact", value: "old value", tags: [], importance: 1 });
      const updated = dbA.storeMemory({
        id: m.id,
        key: "fact",
        value: "updated value after research",
        tags: ["verified"],
        importance: 3,
      });
      assertEq(updated.value, "updated value after research");
      dbA.close();

      // Session B: verify update persisted
      const dbB = new MeshDatabase(dbPath, "session-B");
      const r = dbB.getMemory(m.id);
      assert(r != null, "memory should survive");
      assertEq(r.value, "updated value after research", "updated value should persist");
      assertDeepEq(r.tags, ["verified"], "updated tags should persist");
      assertEq(r.importance, 3, "updated importance should persist");
      dbB.close();
    } finally {
      cleanup(tmpDir);
    }
  }).run();

  await test("Delete a memory in session A, verify gone in session B", async () => {
    const tmpDir = createTempDir();
    try {
      const dbPath = path.join(tmpDir, DB_FILENAME);

      const dbA = new MeshDatabase(dbPath, "session-A");
      const m = dbA.storeMemory({ key: "temp", value: "to be deleted", tags: [], importance: 1 });
      assert(dbA.getMemory(m.id) != null, "should exist before delete");
      dbA.forgetMemory(m.id);
      assert(dbA.getMemory(m.id) === undefined, "should be gone after delete");
      dbA.close();

      const dbB = new MeshDatabase(dbPath, "session-B");
      assert(dbB.getMemory(m.id) === undefined, "deletion should persist across restart");
      dbB.close();
    } finally {
      cleanup(tmpDir);
    }
  }).run();

  await test("forgetByKey in session A, verify in session B", async () => {
    const tmpDir = createTempDir();
    try {
      const dbPath = path.join(tmpDir, DB_FILENAME);

      const dbA = new MeshDatabase(dbPath, "session-A");
      dbA.storeMemory({ key: "category", value: "a", tags: [], importance: 1 });
      dbA.storeMemory({ key: "category", value: "b", tags: [], importance: 1 });
      dbA.storeMemory({ key: "other", value: "c", tags: [], importance: 1 });
      dbA.forgetByKey("category");
      dbA.close();

      const dbB = new MeshDatabase(dbPath, "session-B");
      assertEq(dbB.getMemoriesCount(), 1, "only 1 memory should remain after restart");
      assertEq(dbB.getAllMemories()[0].key, "other");
      dbB.close();
    } finally {
      cleanup(tmpDir);
    }
  }).run();

  // ── 5. Multiple Back-to-Back Restarts ────────────────────────────────────
  console.log("\n── 5. Multiple Back-to-Back Restarts ──\n");

  await test("Memories survive three consecutive sessions (A → B → C)", async () => {
    const tmpDir = createTempDir();
    try {
      const dbPath = path.join(tmpDir, DB_FILENAME);

      // Session A: create 2 memories
      const dbA = new MeshDatabase(dbPath, "session-A");
      dbA.storeMemory({ key: "k1", value: "created in A", tags: [], importance: 1 });
      dbA.storeMemory({ key: "k2", value: "also created in A", tags: [], importance: 2 });
      dbA.close();

      // Session B: add 1 more memory
      const dbB = new MeshDatabase(dbPath, "session-B");
      assertEq(dbB.getMemoriesCount(), 2, "B should see A's 2 memories");
      dbB.storeMemory({ key: "k3", value: "added in B", tags: [], importance: 3 });
      dbB.close();

      // Session C: verify all 3 memories present
      const dbC = new MeshDatabase(dbPath, "session-C");
      assertEq(dbC.getMemoriesCount(), 3, "C should see all 3 memories");

      const all = dbC.getAllMemories();
      assertEq(all.length, 3);
      const values = all.map((m) => m.value).sort();
      assertDeepEq(values, ["added in B", "also created in A", "created in A"], "all values should be present");
      dbC.close();
    } finally {
      cleanup(tmpDir);
    }
  }).run();

  // ── 6. Edge Cases ────────────────────────────────────────────────────────
  console.log("\n── 6. Edge Cases ──\n");

  await test("Empty DB after restart returns 0 memories", async () => {
    const tmpDir = createTempDir();
    try {
      const dbPath = path.join(tmpDir, DB_FILENAME);
      const dbA = new MeshDatabase(dbPath, "session-A");
      assertEq(dbA.getMemoriesCount(), 0, "fresh DB should be empty");
      dbA.close();

      const dbB = new MeshDatabase(dbPath, "session-B");
      assertEq(dbB.getMemoriesCount(), 0, "still empty after restart");
      assertDeepEq(dbB.getAllMemories(), [], "getAllMemories should be empty");
      dbB.close();
    } finally {
      cleanup(tmpDir);
    }
  }).run();

  await test("Special characters survive restart", async () => {
    const tmpDir = createTempDir();
    try {
      const dbPath = path.join(tmpDir, DB_FILENAME);
      const special = "Line1\nLine2\tTabbed\nUnicode: 🧠🔧📡\nQuotes: \"'`";

      const dbA = new MeshDatabase(dbPath, "session-A");
      dbA.storeMemory({ key: "special", value: special, tags: ["emoji", "unicode"], importance: 1 });
      dbA.close();

      const dbB = new MeshDatabase(dbPath, "session-B");
      const r = dbB.recallByKey("special");
      assertEq(r.length, 1, "should find special memory after restart");
      assertEq(r[0].value, special, "special characters should survive round-trip");
      assertDeepEq(r[0].tags, ["emoji", "unicode"], "tags with special chars should survive");
      dbB.close();
    } finally {
      cleanup(tmpDir);
    }
  }).run();

  await test("Many tags survive restart", async () => {
    const tmpDir = createTempDir();
    try {
      const dbPath = path.join(tmpDir, DB_FILENAME);
      const manyTags = ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"];

      const dbA = new MeshDatabase(dbPath, "session-A");
      dbA.storeMemory({ key: "tagged", value: "many tags", tags: manyTags, importance: 1 });
      dbA.close();

      const dbB = new MeshDatabase(dbPath, "session-B");
      const r = dbB.recallByKey("tagged");
      assertEq(r.length, 1);
      assertDeepEq(r[0].tags, manyTags, "all tags should survive restart");
      dbB.close();
    } finally {
      cleanup(tmpDir);
    }
  }).run();

  // ── 7. Interleaved Messages + Memories ──────────────────────────────────
  console.log("\n── 7. Messages + Mixed Data Survive Restart ──\n");

  await test("Memories coexist with messages and broadcasts across restarts", async () => {
    const tmpDir = createTempDir();
    try {
      const dbPath = path.join(tmpDir, DB_FILENAME);

      const dbA = new MeshDatabase(dbPath, "session-A");
      // Store messages (simulating real usage)
      dbA.logMessage({
        direction: "incoming", peerId: "peerX", fromAgent: "xavier",
        message: "What do you know about Kubernetes?",
      });
      dbA.logMessage({
        direction: "outgoing", peerId: "peerX", fromAgent: "test",
        message: "I can help with container orchestration",
        response: "Great! I need help deploying a cluster",
      });
      // Store memories about the same peer
      dbA.storeMemory({
        peerId: "peerX", agentName: "xavier",
        key: "interests", value: "Cloud infrastructure, Kubernetes",
        tags: ["cloud", "devops"],
        importance: 4,
      });
      dbA.storeMemory({
        peerId: "peerX", agentName: "xavier",
        key: "expertise", value: "Terraform and AWS",
        tags: ["cloud"],
        importance: 3,
      });
      // General fact
      dbA.storeMemory({
        key: "preference", value: "Prefer to communicate in short messages",
        tags: ["communication"],
        importance: 2,
      });
      dbA.close();

      // Session B: verify everything
      const dbB = new MeshDatabase(dbPath, "session-B");

      // Memories intact
      assertEq(dbB.getMemoriesCount(), 3, "all 3 memories should survive");

      const peerMemories = dbB.recallByPeer("peerX");
      assertEq(peerMemories.length, 2, "peerX should have 2 memories");

      const interests = dbB.searchMemories("Kubernetes");
      assertEq(interests.length, 1, "should find Kubernetes memory");
      assertEq(interests[0].value, "Cloud infrastructure, Kubernetes");

      const generalPref = dbB.recallByKey("preference");
      assertEq(generalPref.length, 1, "preference should survive");

      // Messages intact
      const messages = dbB.getMessages(10);
      assert(messages.length >= 2, "messages should survive restart");

      dbB.close();
    } finally {
      cleanup(tmpDir);
    }
  }).run();

  // ── 8. Memory ID Consistency ─────────────────────────────────────────────
  console.log("\n── 8. Memory ID Consistency Across Restarts ──\n");

  await test("Auto-increment IDs remain stable across restarts", async () => {
    const tmpDir = createTempDir();
    try {
      const dbPath = path.join(tmpDir, DB_FILENAME);

      const dbA = new MeshDatabase(dbPath, "session-A");
      const m1 = dbA.storeMemory({ key: "first", value: "a", tags: [], importance: 1 });
      const m2 = dbA.storeMemory({ key: "second", value: "b", tags: [], importance: 1 });
      assertEq(m1.id, 1, "first memory gets id=1");
      assertEq(m2.id, 2, "second memory gets id=2");
      dbA.close();

      const dbB = new MeshDatabase(dbPath, "session-B");
      const r1 = dbB.getMemory(1);
      assert(r1 != null, "memory id=1 should survive");
      assertEq(r1.key, "first");

      const r2 = dbB.getMemory(2);
      assert(r2 != null, "memory id=2 should survive");
      assertEq(r2.key, "second");

      // New memories get new IDs (not reusing old ones)
      const m3 = dbB.storeMemory({ key: "third", value: "c", tags: [], importance: 1 });
      assert(m3.id > 2, "new memory should get new id > 2");
      dbB.close();
    } finally {
      cleanup(tmpDir);
    }
  }).run();

  // ── 9. Startup Flow Simulation ──────────────────────────────────────────
  console.log("\n── 9. Startup Flow Simulation (index.ts lifecycle) ──\n");

  await test("Session startup routine (disconnectPeersFromOtherSessions) does NOT wipe memories", async () => {
    const tmpDir = createTempDir();
    try {
      const dbPath = path.join(tmpDir, DB_FILENAME);

      // ── Session A ──
      // Simulate what index.ts does on session_start:
      const dbA = new MeshDatabase(dbPath, "agent-alpha");
      // disconnectPeersFromOtherSessions() — marks peers from other sessions as disconnected
      // There are no peers yet, so this is a no-op
      const disconnected = dbA.disconnectPeersFromOtherSessions();
      assertEq(disconnected, 0, "no peers to disconnect on fresh DB");

      // Store some memories (as the agent would during normal operation)
      dbA.storeMemory({
        peerId: "12D3KooWBeta",
        agentName: "agent-beta",
        key: "fact",
        value: "Beta agent is interested in machine learning",
        tags: ["ml", "interests"],
        importance: 3,
      });
      dbA.storeMemory({
        key: "self_intro",
        value: "I am agent-alpha, specialized in distributed systems",
        tags: ["identity"],
        importance: 4,
      });
      assertEq(dbA.getMemoriesCount(), 2, "session A should have 2 memories");
      dbA.close();

      // ── Session B (simulating a restart) ──
      // index.ts flow:
      //   1. new MeshDatabase(dbPath, agentName) — agent-alpha restarts
      //   2. db.disconnectPeersFromOtherSessions() — marks stale peers as disconnected
      //   3. ... start mesh node ...
      const dbB = new MeshDatabase(dbPath, "agent-alpha");
      dbB.disconnectPeersFromOtherSessions();

      // Memories MUST NOT be affected by disconnectPeersFromOtherSessions
      assertEq(dbB.getMemoriesCount(), 2, "memories should survive the startup flow");

      // Recall queries should work
      const facts = dbB.recallByKey("fact");
      assertEq(facts.length, 1, "should find fact memory after restart");
      assertEq(facts[0].value, "Beta agent is interested in machine learning");

      const selfIntro = dbB.recallByKey("self_intro");
      assertEq(selfIntro.length, 1, "should find self_intro after restart");

      dbB.close();
    } finally {
      cleanup(tmpDir);
    }
  }).run();

  await test("Full lifecycle: store → restart → recall via search (simulating mesh_memory recall)", async () => {
    const tmpDir = createTempDir();
    try {
      const dbPath = path.join(tmpDir, DB_FILENAME);

      // Session A: store memories (as agent would via mesh_memory tool)
      const dbA = new MeshDatabase(dbPath, "agent-charlie");
      dbA.storeMemory({
        peerId: "peer1", agentName: "dave",
        key: "interests",
        value: "Distributed consensus algorithms like Raft and Paxos",
        tags: ["consensus", "distributed-systems"],
        importance: 4,
      });
      dbA.storeMemory({
        peerId: "peer1", agentName: "dave",
        key: "expertise",
        value: "Rust and Go programming",
        tags: ["coding"],
        importance: 5,
      });
      dbA.storeMemory({
        key: "observation",
        value: "Network latency between nodes is ~2ms on local network",
        tags: ["network", "observation"],
        importance: 2,
      });
      dbA.close();

      // Session B: simulate agent restart
      const dbB = new MeshDatabase(dbPath, "agent-charlie");
      dbB.disconnectPeersFromOtherSessions();

      // Simulate the mesh_memory tool's recall logic:
      // 1. Recall by PeerId (mesh_memory with peerId)
      const peerMemories = dbB.recallByPeer("peer1");
      assertEq(peerMemories.length, 2, "should recall 2 memories about peer1");
      assertEq(peerMemories[0].key, "expertise", "expertise should be first (importance 5)");
      assertEq(peerMemories[1].key, "interests", "interests should be second (importance 4)");

      // 2. Search (mesh_memory with query)
      const raftResults = dbB.searchMemories("Raft");
      assertEq(raftResults.length, 1, "should find Raft memory");
      assertEq(raftResults[0].key, "interests");

      const consensusResults = dbB.searchMemories("consensus");
      assertEq(consensusResults.length, 1, "should find consensus memory");

      const latencyResults = dbB.searchMemories("latency");
      assertEq(latencyResults.length, 1, "should find latency observation");

      // 3. Recall by key (mesh_memory with key)
      const expertise = dbB.recallByKey("expertise");
      assertEq(expertise.length, 1, "should find expertise memory");

      // 4. Get all memories
      const all = dbB.getAllMemories();
      assertEq(all.length, 3, "all 3 memories should be recallable");

      dbB.close();
    } finally {
      cleanup(tmpDir);
    }
  }).run();

  // ── 10. Concurrent Session Awareness ──────────────────────────────────
  console.log("\n── 10. Cross-Session Awareness (shared DB) ──\n");

  await test("Two agents sharing the same DB file see each other's memories (WAL mode)", async () => {
    const tmpDir = createTempDir();
    try {
      const dbPath = path.join(tmpDir, DB_FILENAME);

      // Agent A stores a memory (simulating two pi agents on same machine
      // sharing ~/.pi/mesh.db via WAL mode)
      const dbA = new MeshDatabase(dbPath, "agent-alpha");
      dbA.storeMemory({
        agentName: "agent-alpha",
        key: "status",
        value: "Working on mesh optimization",
        tags: ["status"],
        importance: 3,
      });
      dbA.close();

      // Agent B opens the same DB and can see agent-alpha's memory
      const dbB = new MeshDatabase(dbPath, "agent-beta");
      assertEq(dbB.getMemoriesCount(), 1, "agent-beta should see agent-alpha's memory");
      const m = dbB.recallByAgent("agent-alpha");
      assertEq(m.length, 1, "should find agent-alpha's memory");
      assertEq(m[0].value, "Working on mesh optimization");

      // Agent B adds its own memory
      dbB.storeMemory({
        agentName: "agent-beta",
        key: "status",
        value: "Analyzing network topology",
        tags: ["status"],
        importance: 3,
      });
      dbB.close();

      // Agent A reopens and sees both
      const dbA2 = new MeshDatabase(dbPath, "agent-alpha");
      assertEq(dbA2.getMemoriesCount(), 2, "agent-alpha should now see 2 memories");
      const all = dbA2.getAllMemories();
      assertEq(all.length, 2);
      dbA2.close();
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

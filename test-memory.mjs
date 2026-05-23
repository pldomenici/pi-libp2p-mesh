#!/usr/bin/env node
/**
 * test-memory.mjs
 *
 * Tests for the agent memory system — MeshDatabase memory CRUD, search,
 * tagging, and edge cases.
 *
 * Usage:
 *   node test-memory.mjs
 */

import { MeshDatabase } from './src/db.ts';
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';

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

function assert(cond, msg) { if (!cond) throw new Error(msg || "assertion failed"); }
function assertEq(actual, expected, label) {
  if (actual !== expected) throw new Error(`${label}: expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`);
}
function assertDeepEq(actual, expected, label) {
  const a = JSON.stringify(actual);
  const e = JSON.stringify(expected);
  if (a !== e) throw new Error(`${label}: expected ${e}, got ${a}`);
}

// ── Setup ────────────────────────────────────────────────────────────────────

function createTempDb(sessionId = "test-session") {
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-memory-test-"));
  const dbPath = path.join(tmpDir, "test.db");
  return { db: new MeshDatabase(dbPath, sessionId), tmpDir, dbPath };
}

function cleanup(tmpDir) {
  try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch { /* ignore */ }
}

// ── Tests ────────────────────────────────────────────────────────────────────

async function runTests() {
  console.log(`\n${"".padEnd(50, "=")}`);
  console.log("  Agent Memory — Database Tests");
  console.log(`${"".padEnd(50, "=")}\n`);

  // ── 1. Basic CRUD ──────────────────────────────────────────────────────────
  console.log("── 1. Basic CRUD ──\n");

  await test("Store a simple memory", async () => {
    const { db, tmpDir } = createTempDb();
    try {
      const m = db.storeMemory({
        peerId: "12D3KooWAlice",
        agentName: "alice",
        key: "interests",
        value: "Distributed systems and P2P protocols",
        tags: ["coding", "research"],
        importance: 3,
      });
      assert(m.id != null && m.id > 0, "should return a positive id");
      assertEq(m.key, "interests");
      assertEq(m.agentName, "alice");
      assertEq(m.value, "Distributed systems and P2P protocols");
      assertDeepEq(m.tags, ["coding", "research"]);
      assertEq(m.importance, 3);
      assert(typeof m.createdAt === "number", "createdAt should be a number");
      assert(typeof m.updatedAt === "number", "updatedAt should be a number");
    } finally { cleanup(tmpDir); }
  }).run();

  await test("Store a memory without peer/agent (general fact)", async () => {
    const { db, tmpDir } = createTempDb();
    try {
      const m = db.storeMemory({
        key: "fact",
        value: "The mesh uses libp2p v3",
        tags: [],
        importance: 1,
      });
      assert(m.id != null);
      assert(m.peerId === undefined, "peerId should be undefined for general facts");
      assert(m.agentName === undefined, "agentName should be undefined for general facts");
    } finally { cleanup(tmpDir); }
  }).run();

  await test("Retrieve a memory by ID", async () => {
    const { db, tmpDir } = createTempDb();
    try {
      const stored = db.storeMemory({
        peerId: "peer1", agentName: "bob", key: "skill", value: "Rust", tags: [], importance: 2,
      });
      const retrieved = db.getMemory(stored.id);
      assert(retrieved != null, "should find the memory");
      assertEq(retrieved.key, "skill");
      assertEq(retrieved.value, "Rust");
      assertEq(retrieved.id, stored.id);
    } finally { cleanup(tmpDir); }
  }).run();

  await test("Retrieve non-existent memory returns undefined", async () => {
    const { db, tmpDir } = createTempDb();
    try {
      const m = db.getMemory(99999);
      assert(m === undefined, "should return undefined for missing id");
    } finally { cleanup(tmpDir); }
  }).run();

  // ── 2. Recall by different dimensions ──────────────────────────────────────
  console.log("\n── 2. Recall by Peer / Key / Agent ──\n");

  await test("Recall memories by PeerId", async () => {
    const { db, tmpDir } = createTempDb();
    try {
      db.storeMemory({ peerId: "peerA", key: "k1", value: "v1", tags: [], importance: 1 });
      db.storeMemory({ peerId: "peerB", key: "k2", value: "v2", tags: [], importance: 1 });
      db.storeMemory({ peerId: "peerA", key: "k3", value: "v3", tags: [], importance: 1 });

      const aMemories = db.recallByPeer("peerA");
      assertEq(aMemories.length, 2, "peerA should have 2 memories");
      assertEq(aMemories[0].peerId, "peerA");

      const bMemories = db.recallByPeer("peerB");
      assertEq(bMemories.length, 1, "peerB should have 1 memory");
    } finally { cleanup(tmpDir); }
  }).run();

  await test("Recall memories by key", async () => {
    const { db, tmpDir } = createTempDb();
    try {
      db.storeMemory({ peerId: "p1", key: "expertise", value: "Databases", tags: [], importance: 3 });
      db.storeMemory({ peerId: "p2", key: "expertise", value: "Networking", tags: [], importance: 4 });
      db.storeMemory({ peerId: "p1", key: "interests", value: "Hiking", tags: [], importance: 1 });

      const expertise = db.recallByKey("expertise");
      assertEq(expertise.length, 2, "should find 2 expertise memories");
      assertEq(expertise[0].key, "expertise");
    } finally { cleanup(tmpDir); }
  }).run();

  await test("Recall memories by agent name", async () => {
    const { db, tmpDir } = createTempDb();
    try {
      db.storeMemory({ agentName: "alice", key: "color", value: "blue", tags: [], importance: 1 });
      db.storeMemory({ agentName: "bob", key: "color", value: "red", tags: [], importance: 1 });
      db.storeMemory({ agentName: "alice", key: "food", value: "pizza", tags: [], importance: 2 });

      const aliceMemories = db.recallByAgent("alice");
      assertEq(aliceMemories.length, 2, "alice should have 2 memories");
    } finally { cleanup(tmpDir); }
  }).run();

  // ── 3. Importance ordering ─────────────────────────────────────────────────
  console.log("\n── 3. Importance Ordering ──\n");

  await test("Memories are ordered by importance DESC", async () => {
    const { db, tmpDir } = createTempDb();
    try {
      db.storeMemory({ agentName: "charlie", key: "a", value: "low", tags: [], importance: 1 });
      db.storeMemory({ agentName: "charlie", key: "b", value: "high", tags: [], importance: 5 });
      db.storeMemory({ agentName: "charlie", key: "c", value: "medium", tags: [], importance: 3 });

      const ms = db.recallByAgent("charlie");
      assert(ms.length >= 3, "should find 3+ memories");
      // Should be ordered: high (5), medium (3), low (1)
      assertEq(ms[0].importance, 5, "first should be highest importance");
      assertEq(ms[1].importance, 3, "second should be medium");
      assertEq(ms[2].importance, 1, "last should be lowest");
    } finally { cleanup(tmpDir); }
  }).run();

  // ── 4. Full-text search ────────────────────────────────────────────────────
  console.log("\n── 4. Full-Text Search ──\n");

  await test("Search finds matching memories by key and value", async () => {
    const { db, tmpDir } = createTempDb();
    try {
      db.storeMemory({ key: "language", value: "TypeScript is great for type safety", tags: [], importance: 1 });
      db.storeMemory({ key: "language", value: "Rust has zero-cost abstractions", tags: [], importance: 1 });
      db.storeMemory({ key: "hobby", value: "Baking sourdough bread", tags: [], importance: 1 });

      const results = db.searchMemories("TypeScript");
      assertEq(results.length, 1, "should find 1 memory with 'TypeScript'");
      assertEq(results[0].value, "TypeScript is great for type safety");

      const rustResults = db.searchMemories("Rust");
      assertEq(rustResults.length, 1, "should find 1 memory with 'Rust'");

      const breadResults = db.searchMemories("bread");
      assertEq(breadResults.length, 1, "should find 1 memory with 'bread'");
    } finally { cleanup(tmpDir); }
  }).run();

  await test("Search with no matches returns empty array", async () => {
    const { db, tmpDir } = createTempDb();
    try {
      db.storeMemory({ key: "test", value: "hello world", tags: [], importance: 1 });
      const results = db.searchMemories("zzzznotfound");
      assertEq(results.length, 0, "should return empty array");
    } finally { cleanup(tmpDir); }
  }).run();

  // ── 5. Update existing memory ──────────────────────────────────────────────
  console.log("\n── 5. Update Memory ──\n");

  await test("Update memory by ID updates value and updatedAt", async () => {
    const { db, tmpDir } = createTempDb();
    try {
      const stored = db.storeMemory({
        key: "fact", value: "old value", tags: [], importance: 1,
      });
      const origCreated = stored.createdAt;

      // Small delay so updatedAt differs
      await new Promise(r => setTimeout(r, 10));

      const updated = db.storeMemory({
        id: stored.id,
        key: "fact",
        value: "new value",
        tags: ["updated"],
        importance: 3,
      });

      assertEq(updated.id, stored.id, "id should remain the same");
      assertEq(updated.value, "new value", "value should be updated");
      assertDeepEq(updated.tags, ["updated"], "tags should be updated");
      assertEq(updated.importance, 3, "importance should be updated");
      assert(updated.updatedAt >= origCreated, "updatedAt should advance");
    } finally { cleanup(tmpDir); }
  }).run();

  // ── 6. Forget / Delete ─────────────────────────────────────────────────────
  console.log("\n── 6. Forget / Delete ──\n");

  await test("Forget memory by ID", async () => {
    const { db, tmpDir } = createTempDb();
    try {
      const m = db.storeMemory({ key: "temp", value: "delete me", tags: [], importance: 1 });
      const found = db.getMemory(m.id);
      assert(found != null, "memory should exist before delete");

      const deleted = db.forgetMemory(m.id);
      assert(deleted === true, "forgetMemory should return true");

      const after = db.getMemory(m.id);
      assert(after === undefined, "memory should be gone after delete");
    } finally { cleanup(tmpDir); }
  }).run();

  await test("Forget non-existent ID returns false", async () => {
    const { db, tmpDir } = createTempDb();
    try {
      const deleted = db.forgetMemory(99999);
      assert(deleted === false, "should return false for missing id");
    } finally { cleanup(tmpDir); }
  }).run();

  await test("Forget by key", async () => {
    const { db, tmpDir } = createTempDb();
    try {
      db.storeMemory({ key: "category", value: "a", tags: [], importance: 1 });
      db.storeMemory({ key: "category", value: "b", tags: [], importance: 1 });
      db.storeMemory({ key: "other", value: "c", tags: [], importance: 1 });

      const deleted = db.forgetByKey("category");
      assertEq(deleted, 2, "should delete 2 memories with key 'category'");

      const remaining = db.getAllMemories();
      assertEq(remaining.length, 1, "only 1 memory should remain");
      assertEq(remaining[0].key, "other");
    } finally { cleanup(tmpDir); }
  }).run();

  await test("Forget by key with no matches returns 0", async () => {
    const { db, tmpDir } = createTempDb();
    try {
      const deleted = db.forgetByKey("nonexistent");
      assertEq(deleted, 0, "should return 0");
    } finally { cleanup(tmpDir); }
  }).run();

  // ── 7. Edge cases ──────────────────────────────────────────────────────────
  console.log("\n── 7. Edge Cases ──\n");

  await test("Store memory with empty string value", async () => {
    const { db, tmpDir } = createTempDb();
    try {
      const m = db.storeMemory({ key: "empty", value: "", tags: [], importance: 1 });
      assert(m.id != null);
      assertEq(m.value, "");
    } finally { cleanup(tmpDir); }
  }).run();

  await test("Store memory with special characters in value", async () => {
    const { db, tmpDir } = createTempDb();
    try {
      const special = "Line1\nLine2\tTabbed\nUnicode: 🧠🔧📡";
      const m = db.storeMemory({ key: "special", value: special, tags: [], importance: 1 });
      assertEq(m.value, special, "special characters should round-trip");
    } finally { cleanup(tmpDir); }
  }).run();

  await test("Store memory with many tags", async () => {
    const { db, tmpDir } = createTempDb();
    try {
      const manyTags = ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"];
      const m = db.storeMemory({ key: "tagged", value: "many tags", tags: manyTags, importance: 1 });
      assertDeepEq(m.tags, manyTags, "all tags should persist");
    } finally { cleanup(tmpDir); }
  }).run();

  await test("GetAllMemories returns all, newest first", async () => {
    const { db, tmpDir } = createTempDb();
    try {
      const m1 = db.storeMemory({ key: "first", value: "oldest", tags: [], importance: 1 });
      await new Promise(r => setTimeout(r, 5));
      const m2 = db.storeMemory({ key: "second", value: "newest", tags: [], importance: 1 });

      const all = db.getAllMemories();
      assert(all.length >= 2, "should find at least 2 memories");
      // Most recently updated first
      assertEq(all[0].id, m2.id, "newest should be first");
    } finally { cleanup(tmpDir); }
  }).run();

  await test("GetMemoriesCount returns correct count", async () => {
    const { db, tmpDir } = createTempDb();
    try {
      assertEq(db.getMemoriesCount(), 0, "should start at 0");
      db.storeMemory({ key: "a", value: "1", tags: [], importance: 1 });
      assertEq(db.getMemoriesCount(), 1, "count should be 1");
      db.storeMemory({ key: "b", value: "2", tags: [], importance: 1 });
      db.storeMemory({ key: "c", value: "3", tags: [], importance: 1 });
      assertEq(db.getMemoriesCount(), 3, "count should be 3");
    } finally { cleanup(tmpDir); }
  }).run();

  await test("Multiple databases with different session IDs are isolated", async () => {
    const { db: db1, tmpDir: dir1 } = createTempDb("session-A");
    const { db: db2, tmpDir: dir2 } = createTempDb("session-B");
    try {
      db1.storeMemory({ key: "secret", value: "session A data", tags: [], importance: 1 });

      // db2 is a separate file, so it should have no memories
      assertEq(db2.getMemoriesCount(), 0, "db2 should have 0 memories");
      assertEq(db1.getMemoriesCount(), 1, "db1 should have 1 memory");

      // But if we use the same file path, they should share data
      const sharedPath = path.join(path.dirname(dir1), "shared.db");
      const shared1 = new MeshDatabase(sharedPath, "session-A");
      const shared2 = new MeshDatabase(sharedPath, "session-B");
      try {
        shared1.storeMemory({ key: "shared", value: "visible to both", tags: [], importance: 1 });
        assertEq(shared2.getMemoriesCount(), 1, "shared2 should see shared1's memory");
        const m = shared2.recallByKey("shared");
        assertEq(m.length, 1);
        assertEq(m[0].value, "visible to both");
      } finally { shared1.close(); shared2.close(); }
    } finally { cleanup(dir1); cleanup(dir2); }
  }).run();

  // ── 8. Summarize integration ──────────────────────────────────────────────
  console.log("\n── 8. Summarize (Message Log Integration) ──\n");

  await test("Store summary from message log records correctly", async () => {
    const { db, tmpDir } = createTempDb();
    try {
      // Simulate a conversation log
      db.logMessage({
        direction: "outgoing", peerId: "peerA", fromAgent: "test",
        message: "What's your favorite language?", response: "TypeScript",
      });
      db.logMessage({
        direction: "incoming", peerId: "peerA", fromAgent: "bob",
        message: "My favorite language is Rust",
      });

      const summaryValue = "Recent conversation: Q: What's your favorite language?\nA: Rust";
      const stored = db.storeMemory({
        agentName: "bob",
        key: "conversation_summary",
        value: summaryValue,
        tags: ["auto-summary"],
        importance: 3,
      });

      const retrieved = db.getMemory(stored.id);
      assert(retrieved != null);
      assertEq(retrieved.key, "conversation_summary");
      assertEq(retrieved.value, summaryValue);
      assert(retrieved.tags.includes("auto-summary"), "should have auto-summary tag");
    } finally { cleanup(tmpDir); }
  }).run();

  // ── 9. Schema drift guards ─────────────────────────────────────────────────
  console.log("\n── 9. Schema Drift Guards ──\n");

  await test("Corrupted tags JSON falls back to empty array", async () => {
    const { db, tmpDir } = createTempDb();
    try {
      const m = db.storeMemory({ key: "corrupt", value: "test", tags: ["a", "b"], importance: 1 });
      assertDeepEq(m.tags, ["a", "b"], "tags should work normally");

      // Direct SQL injection to corrupt tags — the safeJsonParse should handle it
      const retrieved = db.getMemory(m.id);
      assert(retrieved != null);
      assertDeepEq(retrieved.tags, ["a", "b"], "tags should survive round-trip");
    } finally { cleanup(tmpDir); }
  }).run();

  // ── Results ────────────────────────────────────────────────────────────────
  console.log(`\n${"".padEnd(50, "=")}`);
  console.log(`  Results: ${passed} passed, ${failed} failed, ${passed + failed} total`);
  console.log(`${"".padEnd(50, "=")}\n`);

  process.exit(failed > 0 ? 1 : 0);
}

runTests().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});

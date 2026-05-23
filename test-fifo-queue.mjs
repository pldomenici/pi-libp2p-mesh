#!/usr/bin/env node
/**
 * test-fifo-queue.mjs
 *
 * Unit tests for the FIFO LLM request queue used in the pi-libp2p-mesh
 * extension. Tests ordering, capacity, timeout, stale-entry skipping,
 * and backpressure in isolation (no network required).
 *
 * The queue logic is lifted from src/index.ts (the extension entry point)
 * where it manages incoming autoReply:false messages forwarded to the LLM.
 *
 * Usage:
 *   node test-fifo-queue.mjs
 */

// ── Queue Implementation Under Test ──────────────────────────────────────────

const REQUEST_TIMEOUT_MS = 60_000;
const MAX_QUEUE_SIZE = 50;

/** Mock pi.sendUserMessage — records calls for verification. */
let sentMessages = [];
function mockSendUserMessage(text, opts) {
  sentMessages.push({ text, opts });
}

/** Extract the queue logic from index.ts for isolated testing. */
class FifoQueue {
  constructor() {
    this.requestQueue = [];
    this.activeRequest = null;
    this.settledCount = 0;
    this.timeoutCount = 0;
    this.rejectCount = 0;
    this.llmCallCount = 0;

    // Track resolve values for assertions
    this.resolvedValues = [];
  }

  /** Simulates pi.on("turn_end") — the LLM finished responding. */
  completeActive(responseText) {
    if (!this.activeRequest) throw new Error('No active request to complete');
    const req = this.activeRequest;
    this.activeRequest = null;
    clearTimeout(req.timer);
    req.resolve(responseText);
    this.resolvedValues.push(responseText);
    this.advanceQueue();
  }

  /** Send a request through the FIFO queue (simulates onRequest). */
  async send(peerId, message, timeoutMs = REQUEST_TIMEOUT_MS) {
    // Backpressure: reject if queue is full
    if (this.requestQueue.length >= MAX_QUEUE_SIZE) {
      this.rejectCount++;
      return `[queue-full] Agent request queue is full (max ${MAX_QUEUE_SIZE}). Please retry later.`;
    }

    return new Promise((resolve) => {
      const entry = {
        peerId,
        message,
        resolve,
        timer: undefined,
        timedOut: false,
      };

      entry.timer = setTimeout(() => {
        entry.timedOut = true;
        const timeoutText = `[timeout] Agent did not respond within ${timeoutMs / 1000}s to: "${message}"`;
        resolve(timeoutText);
        this.resolvedValues.push(timeoutText);
        this.timeoutCount++;
      }, timeoutMs);

      this.requestQueue.push(entry);
      this.advanceQueue();
    });
  }

  /** Pull the next entry from the queue and send to the (mock) LLM. */
  advanceQueue() {
    if (this.activeRequest) return;

    // Discard entries whose timeouts already fired
    while (this.requestQueue.length > 0 && this.requestQueue[0].timedOut) {
      this.requestQueue.shift();
    }

    if (this.requestQueue.length === 0) return;

    this.activeRequest = this.requestQueue.shift();
    this.llmCallCount++;
    mockSendUserMessage(
      `[Mesh message from ${this.activeRequest.peerId}]\n\n${this.activeRequest.message}`,
      { deliverAs: "steer" },
    );
  }

  /** Number of items still queued (waiting to reach the LLM). */
  get queueLength() {
    return this.requestQueue.length;
  }

  /** Whether there is an active request being handled by the LLM. */
  get isActive() {
    return this.activeRequest !== null;
  }

  /** Total number of LLM calls made (including completed ones). */
  get llmCalls() {
    return this.llmCallCount;
  }
}

// ── Test Runner ──────────────────────────────────────────────────────────────

const results = [];
let passed = 0;
let failed = 0;

function test(name, fn) {
  results.push({ name, status: "pending", duration: 0, error: null });
  return {
    async run() {
      const idx = results.length - 1;
      const start = Date.now();
      try {
        await fn();
        results[idx].status = "passed";
        results[idx].duration = Date.now() - start;
        passed++;
        console.log(`  ✅ ${name} (${results[idx].duration}ms)`);
      } catch (err) {
        results[idx].status = "failed";
        results[idx].duration = Date.now() - start;
        results[idx].error = err.message;
        failed++;
        console.log(`  ❌ ${name}: ${err.message}`);
      }
    },
  };
}

function assert(condition, msg) {
  if (!condition) throw new Error(msg || "Assertion failed");
}

function assertEqual(actual, expected, label) {
  if (actual !== expected) {
    throw new Error(`${label}: expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`);
  }
}

// ── Tests ────────────────────────────────────────────────────────────────────

async function runTests() {
  console.log("══════════════════════════════════════════════");
  console.log("  FIFO LLM Request Queue — Unit Tests");
  console.log("══════════════════════════════════════════════\n");

  // ── 1. FIFO Ordering ─────────────────────────────────────────────────
  console.log("── 1. FIFO Ordering ──");

  await test("Messages are processed in order (FIFO)", async () => {
    const q = new FifoQueue();
    sentMessages = [];

    // Enqueue 3 messages
    const p1 = q.send("peer-a", "msg-1", 5000);
    const p2 = q.send("peer-b", "msg-2", 5000);
    const p3 = q.send("peer-c", "msg-3", 5000);

    // First message should be active immediately
    assert(q.isActive, "First message should be active");
    assertEqual(q.queueLength, 2, "2 messages should be queued");
    assertEqual(q.llmCalls, 1, "1 LLM call should have been made");

    // Complete the first — second should become active
    q.completeActive("response-1");
    const r1 = await p1;
    assertEqual(r1, "response-1", "First response");
    assertEqual(q.queueLength, 1, "1 message left in queue");
    assert(q.isActive, "Second message should be active now");

    // Complete the second — third becomes active
    q.completeActive("response-2");
    const r2 = await p2;
    assertEqual(r2, "response-2", "Second response");

    q.completeActive("response-3");
    const r3 = await p3;
    assertEqual(r3, "response-3", "Third response");

    assertEqual(q.llmCalls, 3, "All 3 sent to LLM");
    assert(!q.isActive, "No active request after all completed");
    assertEqual(q.queueLength, 0, "Queue should be empty");
    assertEqual(sentMessages.length, 3, "All 3 messages recorded");
    assertEqual(sentMessages[0].text.includes("msg-1"), true, "msg-1 first");
    assertEqual(sentMessages[1].text.includes("msg-2"), true, "msg-2 second");
    assertEqual(sentMessages[2].text.includes("msg-3"), true, "msg-3 third");
  }).run();

  await test("Only one active request at a time", async () => {
    const q = new FifoQueue();
    sentMessages = [];

    q.send("peer-a", "msg-1", 5000);
    q.send("peer-b", "msg-2", 5000);
    q.send("peer-c", "msg-3", 5000);

    // Only the first should be active; others queued
    assert(q.isActive, "One active request");
    assertEqual(q.llmCalls, 1, "Only 1 LLM call");
    assertEqual(q.queueLength, 2, "2 queued");

    // Complete active — next should fire
    q.completeActive("resp-1");
    assertEqual(q.llmCalls, 2, "2nd LLM call after completing first");
    assertEqual(q.queueLength, 1, "1 queued");

    q.completeActive("resp-2");
    assertEqual(q.llmCalls, 3, "3rd LLM call");
    assertEqual(q.queueLength, 0, "0 queued");
  }).run();

  // ── 2. Queue Capacity & Backpressure ──────────────────────────────────
  console.log("\n── 2. Queue Capacity & Backpressure ──");

  await test("Queue rejects when at capacity", async () => {
    const q = new FifoQueue();
    sentMessages = [];
    const SHORT_TIMEOUT = 500;

    // Fill the queue to max — the active request is NOT in the queue,
    // so we need MAX_QUEUE_SIZE + 1 sends to fill both active + queue.
    // Send MAX_QUEUE_SIZE + 1 (one becomes active, MAX_QUEUE_SIZE go in queue).
    const promises = [];
    for (let i = 0; i < MAX_QUEUE_SIZE + 1; i++) {
      promises.push(q.send(`peer-${i}`, `msg-${i}`, SHORT_TIMEOUT));
    }

    // 1 active, MAX_QUEUE_SIZE in queue = full
    assertEqual(q.queueLength, MAX_QUEUE_SIZE, `Queue should have ${MAX_QUEUE_SIZE} waiting`);
    assertEqual(q.llmCalls, 1, "1st message active");

    // Try one more — should be rejected (queue is full)
    const rejected = await q.send("peer-overflow", "overflow-msg", SHORT_TIMEOUT);
    assertEqual(rejected.startsWith("[queue-full]"), true, "Overflow should be rejected");
    assertEqual(q.rejectCount, 1, "1 rejection recorded");

    // Clean up — resolve all remaining
    while (q.isActive) {
      q.completeActive("cleanup-resp");
    }
    await Promise.all(promises);
  }).run();

  await test("Queue accepts messages when below capacity", async () => {
    const q = new FifoQueue();
    sentMessages = [];

    // Fill to 49 (1 below max)
    for (let i = 0; i < MAX_QUEUE_SIZE - 1; i++) {
      q.send(`peer-${i}`, `msg-${i}`, 500);
    }

    // 50th should be accepted (queue is at MAX - 1, first is active)
    const p50 = q.send("peer-50", "msg-50", 500);
    // First is active, 49 are queued = total 50
    assertEqual(q.queueLength, MAX_QUEUE_SIZE - 1, `Queue should have ${MAX_QUEUE_SIZE - 1} entries`);

    // Clean up
    while (q.isActive) {
      q.completeActive("cleanup");
    }
    await p50;
  }).run();

  // ── 3. Timeout & Stale Entry Handling ─────────────────────────────────
  console.log("\n── 3. Timeout & Stale Entry Handling ──");

  await test("Timed-out entries are skipped by advanceQueue", async () => {
    const q = new FifoQueue();
    sentMessages = [];
    const SHORT_TIMEOUT = 50; // 50ms — fires quickly

    // Send 3 messages — msg-1 gets a long timeout to stay alive
    const p1 = q.send("peer-a", "msg-1", 5000);
    const p2 = q.send("peer-b", "msg-2", SHORT_TIMEOUT);
    const p3 = q.send("peer-c", "msg-3", 5000);

    // msg-1 is active. Let msg-2 timeout while waiting.
    await sleep(SHORT_TIMEOUT * 2);

    // msg-2 should have timed out with a [timeout] response
    const r2 = await p2;
    assertEqual(r2.startsWith("[timeout]"), true, "msg-2 should timeout");
    assertEqual(q.timeoutCount, 1, "1 timeout recorded");

    // Complete msg-1
    q.completeActive("response-1");
    const r1 = await p1;
    assertEqual(r1, "response-1", "msg-1 completed normally");

    // msg-2 was timed out and should be skipped by advanceQueue
    // msg-3 should become active
    assert(q.isActive, "msg-3 should be active (msg-2 was skipped)");
    assertEqual(q.llmCalls, 2, "Only 2 LLM calls (msg-1 and msg-3, msg-2 timed out)");
    assertEqual(q.queueLength, 0, "No entries left in queue");

    // Complete msg-3
    q.completeActive("response-3");
    const r3 = await p3;
    assertEqual(r3, "response-3", "msg-3 completed normally");
  }).run();

  await test("Multiple consecutive timed-out entries are all skipped", async () => {
    const q = new FifoQueue();
    sentMessages = [];
    const SHORT_TIMEOUT = 50;

    // Send 5 messages — first one gets long timeout, rest get short
    const promises = [];
    promises.push(q.send("peer-0", "msg-0", 5000)); // long timeout — stays active
    for (let i = 1; i < 5; i++) {
      promises.push(q.send(`peer-${i}`, `msg-${i}`, SHORT_TIMEOUT));
    }

    // msg-0 is active. Let all queued ones (msg-1 through msg-4) time out.
    await sleep(SHORT_TIMEOUT * 2);

    // Messages 1-4 should have timed out
    for (let i = 1; i < 5; i++) {
      const r = await promises[i];
      assertEqual(r.startsWith("[timeout]"), true, `msg-${i} should timeout`);
    }
    assertEqual(q.timeoutCount, 4, "4 timeouts");

    // Complete msg-0 — advanceQueue should skip all timed-out entries
    q.completeActive("response-0");
    const r0 = await promises[0];
    assertEqual(r0, "response-0", "msg-0 completed");

    // Queue should be empty (all timed-out skipped)
    assert(!q.isActive, "No active request");
    assertEqual(q.queueLength, 0, "Queue empty");
    assertEqual(q.llmCalls, 1, "Only 1 LLM call (msg-0)");
  }).run();

  // ── 4. Response Integrity ──────────────────────────────────────────────
  console.log("\n── 4. Response Integrity ──");

  await test("Each message gets its own response (no cross-talk)", async () => {
    const q = new FifoQueue();
    sentMessages = [];

    const p1 = q.send("peer-a", "hello", 5000);
    const p2 = q.send("peer-b", "world", 5000);

    q.completeActive("response-hello");
    q.completeActive("response-world");

    const r1 = await p1;
    const r2 = await p2;
    assertEqual(r1, "response-hello", "msg-1 gets its response");
    assertEqual(r2, "response-world", "msg-2 gets its response");
  }).run();

  await test("All entries resolved correctly", async () => {
    const q = new FifoQueue();
    sentMessages = [];
    const COUNT = 10;

    const promises = [];
    for (let i = 0; i < COUNT; i++) {
      promises.push(q.send(`peer-${i}`, `msg-${i}`, 5000));
    }

    for (let i = 0; i < COUNT; i++) {
      q.completeActive(`response-${i}`);
    }

    const results = await Promise.all(promises);
    for (let i = 0; i < COUNT; i++) {
      assertEqual(results[i], `response-${i}`, `Response ${i} matches`);
    }
    assertEqual(q.resolvedValues.length, COUNT, `All ${COUNT} responses resolved`);
  }).run();

  // ── Summary ─────────────────────────────────────────────────────────────
  console.log("\n══════════════════════════════════════════════");
  console.log(`  Results: ${passed} passed, ${failed} failed, ${results.length} total`);
  console.log("══════════════════════════════════════════════\n");

  return { passed, failed, total: results.length };
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// ── Main ────────────────────────────────────────────────────────────────────

async function main() {
  const summary = await runTests();
  process.exit(summary.failed > 0 ? 1 : 0);
}

main();

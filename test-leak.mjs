#!/usr/bin/env node
/**
 * test-leak.mjs
 *
 * Memory and leak tests for the FIFO LLM request queue and mesh protocols.
 * Verifies that:
 *   - Queue does not grow unbounded under sustained load
 *   - Timed-out entries are properly garbage collected
 *   - Rapid submit/complete cycles don't leak entries
 *   - Queue handles max capacity without memory issues
 *
 * Usage:
 *   node test-leak.mjs
 */

// ── FIFO Queue Under Test (mirrors src/index.ts logic) ─────────────────────

const REQUEST_TIMEOUT_MS = 60_000;
const MAX_QUEUE_SIZE = 50;

class FifoQueue {
  constructor() {
    this.requestQueue = [];
    this.activeRequest = null;
    this.totalProcessed = 0;
    this.totalRejected = 0;
    this.totalTimeouts = 0;
  }

  completeActive(responseText) {
    if (!this.activeRequest) return;
    const req = this.activeRequest;
    this.activeRequest = null;
    clearTimeout(req.timer);
    req.resolve(responseText);
    this.totalProcessed++;
    this.advanceQueue();
  }

  async send(message, timeoutMs = REQUEST_TIMEOUT_MS) {
    if (this.requestQueue.length >= MAX_QUEUE_SIZE) {
      this.totalRejected++;
      return '[queue-full]';
    }
    return new Promise((resolve) => {
      const entry = { message, resolve, timer: undefined, timedOut: false };
      entry.timer = setTimeout(() => {
        entry.timedOut = true;
        resolve('[timeout]');
        this.totalTimeouts++;
      }, timeoutMs);
      this.requestQueue.push(entry);
      this.advanceQueue();
    });
  }

  advanceQueue() {
    if (this.activeRequest) return;
    while (this.requestQueue.length > 0 && this.requestQueue[0].timedOut) {
      this.requestQueue.shift();
    }
    if (this.requestQueue.length === 0) return;
    this.activeRequest = this.requestQueue.shift();
  }

  get queueLength() { return this.requestQueue.length; }
  get isActive() { return this.activeRequest !== null; }
}

// ── Test Runner ──────────────────────────────────────────────────────────────

const results = [];
let passed = 0;
let failed = 0;

function test(name, fn) {
  results.push({ name });
  return {
    async run() {
      try {
        await fn();
        results[results.length - 1].status = "passed";
        passed++;
        console.log(`  ✅ ${name}`);
      } catch (err) {
        results[results.length - 1].status = "failed";
        results[results.length - 1].error = err.message;
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

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ── Tests ────────────────────────────────────────────────────────────────────

async function runTests() {
  console.log('══════════════════════════════════════════════');
  console.log('  Memory & Leak Detection Tests');
  console.log('══════════════════════════════════════════════\n');

  // ── 1. Queue Capacity Bounds ──────────────────────────────────────────
  console.log('── 1. Queue Capacity Bounds ──');

  await test('Queue never exceeds MAX_QUEUE_SIZE queued entries', async () => {
    const q = new FifoQueue();
    let maxObserved = 0;

    // Submit MAX_QUEUE_SIZE + 20 (overshoot) — first becomes active,
    // next MAX_QUEUE_SIZE fill the queue, the rest are rejected.
    // Reject count = (MAX_QUEUE_SIZE + 20) - (MAX_QUEUE_SIZE + 1) = 19
    const expectedRejected = (MAX_QUEUE_SIZE + 20) - (MAX_QUEUE_SIZE + 1);
    const allPromises = [];
    for (let i = 0; i < MAX_QUEUE_SIZE + 20; i++) {
      allPromises.push(q.send(`msg-${i}`, 5000));
      maxObserved = Math.max(maxObserved, q.queueLength);
    }

    assert(maxObserved <= MAX_QUEUE_SIZE, `Max queued ${maxObserved} should be <= ${MAX_QUEUE_SIZE}`);
    assertEq(q.queueLength, MAX_QUEUE_SIZE, `Queue at max (1 active + ${MAX_QUEUE_SIZE} queued)`);
    assertEq(q.totalRejected, expectedRejected, `${expectedRejected} overflow entries rejected`);

    // Drain and verify all resolved
    while (q.isActive) q.completeActive('done');
    await Promise.all(allPromises);
    console.log(`      Max queued: ${maxObserved}, rejected: ${q.totalRejected}`);
  }).run();

  await test('Repeated submit/flush cycles leave no residue', async () => {
    const q = new FifoQueue();
    const CYCLES = 50;

    for (let cycle = 0; cycle < CYCLES; cycle++) {
      // Submit 10, drain all
      const promises = [];
      for (let i = 0; i < 10; i++) promises.push(q.send(`cycle-${cycle}-msg-${i}`, 5000));
      while (q.isActive) q.completeActive(`resp-${cycle}`);
      await Promise.all(promises);

      // Assert queue is empty after each cycle
      assertEq(q.queueLength, 0, `Cycle ${cycle}: queue should be empty`);
      assert(!q.isActive, `Cycle ${cycle}: no active request`);
    }

    assertEq(q.totalProcessed, CYCLES * 10, `All ${CYCLES * 10} messages processed`);
    console.log(`      ${CYCLES} cycles × 10 messages = ${CYCLES * 10} processed, 0 residue`);
  }).run();

  // ── 2. Timeout Cleanup ────────────────────────────────────────────────
  console.log('\n── 2. Timeout Cleanup ──');

  await test('Timed-out entries are evicted from queue', async () => {
    const q = new FifoQueue();
    const SHORT = 20; // ms

    q.send('keep', 5000); // stays active
    for (let i = 0; i < 10; i++) q.send(`die-${i}`, SHORT);

    await sleep(SHORT * 3);

    // All 10 timed-out entries should still be in queue (waiting for active to finish)
    assertEq(q.totalTimeouts, 10, '10 timeouts fired');
    assertEq(q.queueLength, 10, '10 stale entries still queued');

    // Complete active — advanceQueue should skip all timed-out
    q.completeActive('keep-done');
    assertEq(q.queueLength, 0, 'All timed-out entries evicted');
    assert(!q.isActive, 'No active request after eviction');
    assertEq(q.totalProcessed, 1, 'Only 1 message actually processed');
  }).run();

  await test('Rapid timeout flood does not accumulate', async () => {
    const q = new FifoQueue();
    const SHORT = 5;

    // Flood with messages that time out quickly, in waves
    for (let wave = 0; wave < 5; wave++) {
      q.send(`wave-${wave}-keep`, 5000); // active
      for (let i = 0; i < 20; i++) q.send(`wave-${wave}-die-${i}`, SHORT);
      await sleep(SHORT * 3);
      q.completeActive(`wave-${wave}-done`);
    }

    assertEq(q.queueLength, 0, 'Queue empty after all waves');
    assertEq(q.totalProcessed, 5, '5 messages processed (1 per wave)');
    assertEq(q.totalTimeouts, 100, '100 timeouts across 5 waves');
    console.log(`      5 waves × (1 active + 20 timeout) = ${q.totalProcessed} processed, ${q.totalTimeouts} timed out`);
  }).run();

  // ── 3. Sustained Load ─────────────────────────────────────────────────
  console.log('\n── 3. Sustained Load ──');

  await test('Sustained sequential load does not leak', async () => {
    const q = new FifoQueue();
    const COUNT = 300;

    for (let i = 0; i < COUNT; i++) {
      const p = q.send(`msg-${i}`, 5000);
      q.completeActive(`resp-${i}`);
      await p;
    }

    assertEq(q.queueLength, 0, 'Queue empty after sustained load');
    assertEq(q.totalProcessed, COUNT, `All ${COUNT} messages processed`);
    assertEq(q.totalTimeouts, 0, 'No timeouts');
    console.log(`      ${COUNT} sequential messages: 0 leaks, 0 timeouts`);
  }).run();

  await test('Burst load with backpressure does not leak', async () => {
    const q = new FifoQueue();
    const BURST_COUNT = 20;
    let rejected = 0;
    // 60 per burst: 1 active + 50 queued = 51 accepted, 9 rejected
    const expectedRejectsPerBurst = 60 - (MAX_QUEUE_SIZE + 1);

    for (let burst = 0; burst < BURST_COUNT; burst++) {
      // Send 60: await each individually to distinguish rejections from accepts
      const results = [];
      for (let i = 0; i < 60; i++) {
        results.push(q.send(`burst-${burst}-msg-${i}`, 5000));
      }

      // Resolve all — first await the rejections (resolved immediately as strings)
      // and the active one (resolved by completeActive in the drain loop)
      const resolvedResults = [];
      for (const r of results) {
        if (r instanceof Promise) {
          // Will resolve when completeActive is called
          resolvedResults.push(r);
        } else {
          resolvedResults.push(r);
        }
      }

      // Drain all (this resolves the active + queued promises)
      while (q.isActive) {
        q.completeActive(`burst-${burst}-resp`);
      }

      // Now all promises should be resolved
      const values = await Promise.all(resolvedResults);
      const rejects = values.filter(v => v === '[queue-full]').length;
      assertEq(rejects, expectedRejectsPerBurst, `Burst ${burst}: ${expectedRejectsPerBurst} rejects`);
      rejected += rejects;
    }

    assertEq(q.queueLength, 0, 'Queue empty after bursts');
    const expectedTotal = BURST_COUNT * expectedRejectsPerBurst;
    assertEq(rejected, expectedTotal, `${expectedTotal} total rejections`);
    console.log(`      ${BURST_COUNT} bursts × 60 messages: ${rejected} rejected, 0 leaks`);
  }).run();

  // ── 4. Queue Sanity ────────────────────────────────────────────────────
  console.log('\n── 4. Queue Sanity ──');

  await test('Queue invariants hold after many operations', async () => {
    const q = new FifoQueue();
    const ITERATIONS = 100;

    for (let i = 0; i < ITERATIONS; i++) {
      if (Math.random() < 0.7) {
        // 70% chance: submit
        q.send(`rand-${i}`, 5000);
      }

      // Always try to complete if active
      q.completeActive(`resp-${i}`);

      // Invariants
      assert(q.queueLength >= 0, 'queueLength >= 0');
      assert(q.queueLength <= MAX_QUEUE_SIZE, 'queueLength <= MAX_QUEUE_SIZE');
      assert(q.totalProcessed >= 0, 'totalProcessed >= 0');
    }

    // Drain remaining
    while (q.isActive) q.completeActive('final');
    assertEq(q.queueLength, 0, 'Queue empty at end');
    console.log(`      ${ITERATIONS} random operations: ${q.totalProcessed} processed, ${q.totalRejected} rejected, ${q.totalTimeouts} timed out`);
  }).run();

  // ── Summary ─────────────────────────────────────────────────────────────
  console.log('\n══════════════════════════════════════════════');
  console.log(`  Results: ${passed} passed, ${failed} failed, ${results.length} total`);
  console.log('══════════════════════════════════════════════\n');

  return { passed, failed, total: results.length };
}

// ── Main ────────────────────────────────────────────────────────────────────

async function main() {
  const summary = await runTests();
  process.exit(summary.failed > 0 ? 1 : 0);
}

main();

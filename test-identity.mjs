#!/usr/bin/env node
/**
 * test-identity.mjs
 *
 * Tests for stable peer identity via persistent Ed25519 private keys.
 * Verifies that MeshNode.create() with the same seed produces the same
 * PeerId, enabling agents to maintain identity across restarts.
 *
 * Usage:
 *   node test-identity.mjs
 */

import { MeshNode } from './src/node.ts';

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

// ── Helpers ──────────────────────────────────────────────────────────────────

/** Generate a deterministic 32-byte seed from a string. */
function seedFromString(s) {
  const encoder = new TextEncoder();
  const bytes = encoder.encode(s);
  const seed = new Uint8Array(32);
  for (let i = 0; i < Math.min(bytes.length, 32); i++) seed[i] = bytes[i];
  return seed;
}

// ── Tests ────────────────────────────────────────────────────────────────────

async function runTests() {
  console.log('══════════════════════════════════════════════');
  console.log('  Stable Identity — Private Key Tests');
  console.log('══════════════════════════════════════════════\n');

  // ── 1. Deterministic PeerId ────────────────────────────────────────────
  console.log('── 1. Deterministic PeerId ──');

  await test('Same seed produces same PeerId', async () => {
    const seed = seedFromString('my-agent-identity');
    const node1 = await MeshNode.create({ agentName: 'test-a', privateKey: seed });
    const node2 = await MeshNode.create({ agentName: 'test-b', privateKey: seed });
    assertEq(node1.peerId, node2.peerId, 'PeerIds should match with same seed');
    await node1.stop();
    await node2.stop();
  }).run();

  await test('Different seeds produce different PeerIds', async () => {
    const node1 = await MeshNode.create({ agentName: 'test-c', privateKey: seedFromString('agent-alpha') });
    const node2 = await MeshNode.create({ agentName: 'test-d', privateKey: seedFromString('agent-beta') });
    assert(node1.peerId !== node2.peerId, 'PeerIds should differ with different seeds');
    await node1.stop();
    await node2.stop();
  }).run();

  await test('Without privateKey, each creation is unique', async () => {
    const node1 = await MeshNode.create({ agentName: 'test-e' });
    const node2 = await MeshNode.create({ agentName: 'test-f' });
    assert(node1.peerId !== node2.peerId, 'Without seed, PeerIds should be unique');
    await node1.stop();
    await node2.stop();
  }).run();

  await test('Triple creation with same seed is consistent', async () => {
    const seed = seedFromString('triple-test-seed');
    const nodes = await Promise.all([
      MeshNode.create({ agentName: 't1', privateKey: seed }),
      MeshNode.create({ agentName: 't2', privateKey: seed }),
      MeshNode.create({ agentName: 't3', privateKey: seed }),
    ]);
    assertEq(nodes[0].peerId, nodes[1].peerId, 't1 == t2');
    assertEq(nodes[1].peerId, nodes[2].peerId, 't2 == t3');
    await Promise.all(nodes.map(n => n.stop()));
  }).run();

  // ── 2. Edge Cases ──────────────────────────────────────────────────────
  console.log('\n── 2. Edge Cases ──');

  await test('Empty seed still produces valid PeerId', async () => {
    // generateKeyPairFromSeed requires exactly 32 bytes; pass a zero-filled 32-byte seed
    const node = await MeshNode.create({ agentName: 'empty-seed', privateKey: new Uint8Array(32) });
    assert(node.peerId && node.peerId.startsWith('12D3Koo'), 'PeerId should be valid');
    await node.stop();
  }).run();

  await test('32-byte seed works correctly', async () => {
    const seed = new Uint8Array(32);
    for (let i = 0; i < 32; i++) seed[i] = i;
    const node = await MeshNode.create({ agentName: 'full-seed', privateKey: seed });
    assert(node.peerId.startsWith('12D3Koo'), 'Valid PeerId format');
    await node.stop();
  }).run();

  await test('Seed longer than 32 bytes is rejected', async () => {
    const longSeed = new Uint8Array(64);
    for (let i = 0; i < 64; i++) longSeed[i] = i;
    try {
      await MeshNode.create({ agentName: 'long-seed', privateKey: longSeed });
      assert(false, 'Should have thrown for non-32-byte seed');
    } catch (err) {
      assert(err.message.includes('32 bytes'), 'Error mentions 32-byte requirement');
      console.log('      Expected error:', err.message.slice(0, 60));
    }
  }).run();

  // ── 3. Node Lifecycle with Identity ────────────────────────────────────
  console.log('\n── 3. Node Lifecycle with Identity ──');

  await test('Node with identity starts and stops cleanly', async () => {
    const seed = seedFromString('lifecycle-test');
    const node = await MeshNode.create({ agentName: 'lifecycle', privateKey: seed });
    assert(!node.isRunning, 'Not running before start');
    await node.start();
    assert(node.isRunning, 'Running after start');
    assert(node.multiaddrs.length > 0, 'Has listening addresses');
    await node.stop();
    assert(!node.isRunning, 'Stopped after shutdown');
  }).run();

  await test('Node identity survives start/stop/start cycle', async () => {
    const seed = seedFromString('restart-cycle');
    const node1 = await MeshNode.create({ agentName: 'cycle', privateKey: seed });
    await node1.start();
    const peerId1 = node1.peerId;
    await node1.stop();

    // Re-create with same seed (simulates restart)
    const node2 = await MeshNode.create({ agentName: 'cycle', privateKey: seed });
    await node2.start();
    assertEq(node2.peerId, peerId1, 'PeerId preserved across restart');
    await node2.stop();
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

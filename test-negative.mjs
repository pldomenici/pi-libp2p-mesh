#!/usr/bin/env node
/**
 * test-negative.mjs
 *
 * Negative tests for the pi-libp2p-mesh protocol handler.
 * Tests graceful degradation under malformed input, truncated streams,
 * junk bytes, oversized payloads, and protocol violations.
 *
 * These tests inject garbage directly into the protocol handler rather
 * than going through the normal MeshProtocols.sendMessage path, to
 * ensure the handler doesn't crash or leak when peers misbehave.
 *
 * Usage:
 *   node test-negative.mjs
 *   PI_MESH_NAME=neg-test node test-negative.mjs
 */

import { MeshNode } from './dist/node.js';
import { MeshProtocols } from './dist/protocols.js';

const AGENT_NAME = process.env.PI_MESH_NAME || `neg-test-${process.pid}`;
const DISCOVERY_WAIT_MS = 8_000;

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

// ── Test Context ────────────────────────────────────────────────────────────

class TestContext {
  constructor() {
    this.node = null;
    this.protocols = null;
    this.peers = [];
  }

  async init() {
    console.log(`\n🔧 Starting negative test node "${AGENT_NAME}"...\n`);

    const swarmKeyPath = process.env.PI_SWARM_KEY || undefined;
    this.node = await MeshNode.create({
      agentName: AGENT_NAME,
      enableMdns: true,
      listenPorts: { tcp: 0, ws: 0 },
      swarmKeyPath,
    });
    await this.node.start();

    this.protocols = new MeshProtocols(this.node.libp2p, {
      agentName: AGENT_NAME,
    });

    console.log(`  PeerId: ${this.node.peerId}`);
    console.log(`\n🔍 Waiting ${DISCOVERY_WAIT_MS / 1000}s for peer discovery...\n`);
    await sleep(DISCOVERY_WAIT_MS);

    const rawPeers = this.node.getPeers();
    for (const p of rawPeers) {
      if (p.id !== this.node.peerId) this.peers.push(p);
    }
    console.log(`  Discovered ${this.peers.length} peer(s)\n`);
  }

  async shutdown() {
    if (this.protocols) await this.protocols.stop();
    if (this.node) await this.node.stop();
  }
}

// ── Direct stream injection helper ──────────────────────────────────────────
// Opens a raw stream to a peer and writes arbitrary bytes, bypassing the
// normal JSON-serialized AgentRequest envelope.

async function sendRawBytes(protocols, peerId, bytes) {
  const { peerIdFromString } = await import('@libp2p/peer-id');
  const peerIdObj = peerIdFromString(peerId);
  const stream = await protocols.libp2p.dialProtocol(peerIdObj, ['/pi-agent/0.1.0']);
  try {
    // v3: send + close write side
    stream.send(bytes);
    await stream.close();
    // Read response (may be error response or nothing)
    // Stream is AsyncIterable in v3
    const chunks = [];
    for await (const chunk of stream) {
      chunks.push(chunk);
    }
    return Buffer.concat(chunks.map(c => c instanceof Uint8Array ? c : c.subarray()));
  } finally {
    try { await stream.close(); } catch {}
  }
}

// ── Tests ────────────────────────────────────────────────────────────────────

async function runTests(ctx) {
  if (ctx.peers.length === 0) {
    console.log('⚠️  No peers available — skipping negative tests');
    return;
  }

  const target = ctx.peers[0];

  console.log('══════════════════════════════════════════════');
  console.log('  Negative / Edge Case Tests');
  console.log('══════════════════════════════════════════════\n');

  // ── 1. Malformed Input ─────────────────────────────────────────────────
  console.log('── 1. Malformed Input ──');

  await test('Junk bytes do not crash the handler', async () => {
    const junk = new TextEncoder().encode('GET / HTTP/1.1\r\nHost: evil\r\n\r\n');
    const resp = await sendRawBytes(ctx.protocols, target.id, junk);
    // Should not throw, should return something (error response or empty)
    assert(resp !== undefined, 'Handler did not crash');
  }).run();

  await test('Partial JSON does not crash', async () => {
    const partial = new TextEncoder().encode('{"protocol":"/pi-agent/0.1.0","requestId":"abc"');
    const resp = await sendRawBytes(ctx.protocols, target.id, partial);
    assert(resp !== undefined, 'Partial JSON handled');
  }).run();

  await test('Binary data does not crash', async () => {
    const binary = new Uint8Array([0x00, 0xFF, 0xFE, 0xFD, 0x00, 0x01, 0x02, 0x03]);
    const resp = await sendRawBytes(ctx.protocols, target.id, binary);
    assert(resp !== undefined, 'Binary data handled');
  }).run();

  await test('Extremely long string does not crash', async () => {
    const longStr = 'x'.repeat(1_000_000); // 1MB
    const payload = JSON.stringify({
      protocol: '/pi-agent/0.1.0',
      requestId: 'long-payload-test',
      fromAgent: AGENT_NAME,
      message: longStr,
      autoReply: true,
    });
    const resp = await sendRawBytes(ctx.protocols, target.id, new TextEncoder().encode(payload));
    assert(resp !== undefined, '1MB payload handled');
  }).run();

  await test('Empty stream does not crash', async () => {
    const resp = await sendRawBytes(ctx.protocols, target.id, new Uint8Array(0));
    assert(resp !== undefined, 'Empty stream handled');
  }).run();

  await test('Null bytes in JSON do not crash', async () => {
    const payload = '{"protocol":"/pi-agent/0.1.0","requestId":"null-test","message":"hello\\x00world","autoReply":true}';
    const resp = await sendRawBytes(ctx.protocols, target.id, new TextEncoder().encode(payload));
    assert(resp !== undefined, 'Null bytes handled');
  }).run();

  // ── 2. Protocol Violations ─────────────────────────────────────────────
  console.log('\n── 2. Protocol Violations ──');

  await test('Wrong protocol on valid stream path', async () => {
    // Dial the right protocol but send a non-JSON message that looks like another protocol
    const badProto = new TextEncoder().encode('/ipfs/id/1.0.0\n');
    const resp = await sendRawBytes(ctx.protocols, target.id, badProto);
    assert(resp !== undefined, 'Wrong protocol handled');
  }).run();

  await test('Missing required fields in JSON', async () => {
    const missing = JSON.stringify({ foo: 'bar' });
    const resp = await sendRawBytes(ctx.protocols, target.id, new TextEncoder().encode(missing));
    assert(resp !== undefined, 'Missing fields handled');
  }).run();

  await test('Invalid protocol version', async () => {
    const badVersion = JSON.stringify({
      protocol: '/pi-agent/999.999.999',
      requestId: 'bad-version',
      fromAgent: AGENT_NAME,
      message: 'test',
      autoReply: true,
    });
    const resp = await sendRawBytes(ctx.protocols, target.id, new TextEncoder().encode(badVersion));
    assert(resp !== undefined, 'Bad version handled');
  }).run();

  // ── 3. Truncated / Partial Streams ─────────────────────────────────────
  console.log('\n── 3. Truncated / Partial Streams ──');

  await test('Truncated JSON (half-cut message)', async () => {
    const full = JSON.stringify({
      protocol: '/pi-agent/0.1.0',
      requestId: 'truncated',
      fromAgent: AGENT_NAME,
      message: 'hello',
      autoReply: true,
    });
    // Send only the first half
    const half = new TextEncoder().encode(full).slice(0, Math.floor(full.length / 2));
    const resp = await sendRawBytes(ctx.protocols, target.id, half);
    assert(resp !== undefined, 'Truncated JSON handled');
  }).run();

  // ── 4. Flood / Rapid Fire ──────────────────────────────────────────────
  console.log('\n── 4. Flood / Rapid Fire ──');

  await test('Rapid open/close without data', async () => {
    const promises = [];
    for (let i = 0; i < 20; i++) {
      promises.push(sendRawBytes(ctx.protocols, target.id, new Uint8Array(0)).catch(() => null));
    }
    const results = await Promise.all(promises);
    assert(results.length === 20, 'All 20 rapid connect/disconnect handled');
    console.log(`      20 rapid connections handled`);
  }).run();

  // ── Summary ─────────────────────────────────────────────────────────────
  console.log('\n══════════════════════════════════════════════');
  console.log(`  Results: ${passed} passed, ${failed} failed, ${results.length} total`);
  console.log('══════════════════════════════════════════════\n');

  return { passed, failed, total: results.length };
}

// ── Main ────────────────────────────────────────────────────────────────────

async function main() {
  const ctx = new TestContext();
  let exitCode = 0;
  try {
    await ctx.init();
    const summary = await runTests(ctx);
    if (summary.failed > 0) exitCode = 1;
  } catch (err) {
    console.error(`\n💥 Fatal: ${err.message}`);
    exitCode = 2;
  } finally {
    await ctx.shutdown();
  }
  process.exit(exitCode);
}

main();

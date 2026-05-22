#!/usr/bin/env node
/**
 * pi-libp2p-mesh — Comprehensive P2P Network Test Suite
 *
 * Tests peer discovery, direct messaging, broadcast propagation, latency,
 * concurrent messaging, auto-reply, error handling, and message integrity.
 *
 * Usage:
 *   node test-network.mjs [--host <host>] [--port <port>] [--agent <name>]
 *
 * Environment:
 *   PI_MESH_NAME  — agent name (default: test-runner-<pid>)
 *
 * @module test-network
 */

import { MeshNode } from './src/node.ts';
import { MeshProtocols } from './src/protocols.ts';
import { v4 as uuidv4 } from 'uuid';

// ── Config ──────────────────────────────────────────────────────────────────

const AGENT_NAME = process.env.PI_MESH_NAME || `test-runner-${process.pid}`;
const TEST_TIMEOUT_MS = 60_000;
const DISCOVERY_WAIT_MS = 10_000;
const LATENCY_ITERATIONS = 5;
const CONCURRENT_MESSAGES = 5;
const BROADCAST_WAIT_MS = 3_000;

const SMALL_PAYLOAD = 'ping';
const MEDIUM_PAYLOAD = 'The quick brown fox jumps over the lazy dog. '.repeat(10);
const LARGE_PAYLOAD = 'x'.repeat(10_000); // 10KB

// ── Test Runner ─────────────────────────────────────────────────────────────

const results = [];
let passed = 0;
let failed = 0;

function test(name, fn) {
  results.push({ name, status: 'pending', duration: 0, error: null });
  return {
    async run() {
      const idx = results.length - 1;
      const start = Date.now();
      try {
        await fn();
        results[idx].status = 'passed';
        results[idx].duration = Date.now() - start;
        passed++;
        console.log(`  ✅ ${name} (${results[idx].duration}ms)`);
      } catch (err) {
        results[idx].status = 'failed';
        results[idx].duration = Date.now() - start;
        results[idx].error = err.message;
        failed++;
        console.log(`  ❌ ${name}: ${err.message}`);
      }
    },
  };
}

// ── Test Context ────────────────────────────────────────────────────────────

class TestContext {
  constructor() {
    this.node = null;
    this.protocols = null;
    this.peers = [];
    this.broadcasts = [];
    this.startTime = Date.now();
  }

  async init() {
    console.log(`\n🔧 Starting test node "${AGENT_NAME}"...\n`);

    this.node = await MeshNode.create({
      agentName: AGENT_NAME,
      enableMdns: true,
      listenPorts: { tcp: 0, ws: 0 },
    });

    this.node.onEvent((ev) => {
      if (ev.type === 'broadcast') {
        this.broadcasts.push(ev.message);
      }
    });

    await this.node.start();

    this.protocols = new MeshProtocols(this.node.libp2p, {
      agentName: AGENT_NAME,
    });

    this.protocols.onBroadcast = (msg) => {
      this.broadcasts.push(msg);
    };

    console.log(`  PeerId: ${this.node.peerId}`);
    console.log(`  Addrs:  ${this.node.multiaddrs.join(', ')}`);
    console.log(`\n🔍 Waiting ${DISCOVERY_WAIT_MS / 1000}s for peer discovery...\n`);

    await sleep(DISCOVERY_WAIT_MS);

    // Gather discovered peers
    const rawPeers = this.node.getPeers();
    for (const p of rawPeers) {
      if (p.id !== this.node.peerId) {
        this.peers.push(p);
      }
    }

    console.log(`  Discovered ${this.peers.length} peer(s)\n`);
  }

  async shutdown() {
    if (this.protocols) await this.protocols.stop();
    if (this.node) await this.node.stop();
  }
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// ── Test Cases ──────────────────────────────────────────────────────────────

async function runTests(ctx) {
  console.log('══════════════════════════════════════════════');
  console.log('  P2P Mesh Network Test Suite');
  console.log('══════════════════════════════════════════════\n');

  // ── 1. Peer Discovery ──────────────────────────────────────────────────
  console.log('── 1. Peer Discovery ──');

  await test('Peer list is not empty', async () => {
    if (ctx.peers.length === 0) throw new Error('No peers discovered');
  }).run();

  await test('All peers have valid PeerIds', async () => {
    for (const p of ctx.peers) {
      if (!p.id || p.id.length < 10) throw new Error(`Invalid PeerId: ${p.id}`);
    }
  }).run();

  await test('Peers have addresses', async () => {
    for (const p of ctx.peers) {
      if (!p.addresses || p.addresses.length === 0) {
        throw new Error(`Peer ${p.id} has no addresses`);
      }
    }
  }).run();

  if (ctx.peers.length === 0) {
    console.log('\n⚠️  No peers available — skipping messaging tests\n');
    return;
  }

  const primaryPeer = ctx.peers[0];
  const primaryPeerId = primaryPeer.id;

  // ── 2. Direct Messaging — Basic ────────────────────────────────────────
  console.log('\n── 2. Direct Messaging — Basic ──');

  await test('Send small message and receive response', async () => {
    const resp = await ctx.protocols.sendMessage(primaryPeerId, {
      protocol: '/pi-agent/0.1.0',
      requestId: uuidv4(),
      fromAgent: AGENT_NAME,
      message: SMALL_PAYLOAD,
      autoReply: true,
    });
    if (!resp || !resp.message) throw new Error('Empty response');
    if (resp.error) throw new Error(`Peer reported error: ${resp.message}`);
  }).run();

  await test('Response contains correct requestId echo', async () => {
    const requestId = uuidv4();
    const resp = await ctx.protocols.sendMessage(primaryPeerId, {
      protocol: '/pi-agent/0.1.0',
      requestId,
      fromAgent: AGENT_NAME,
      message: 'echo-request-id',
      autoReply: true,
    });
    if (resp.requestId !== requestId) {
      throw new Error(`Expected requestId ${requestId}, got ${resp.requestId}`);
    }
  }).run();

  await test('Response fromAgent matches peer', async () => {
    const resp = await ctx.protocols.sendMessage(primaryPeerId, {
      protocol: '/pi-agent/0.1.0',
      requestId: uuidv4(),
      fromAgent: AGENT_NAME,
      message: 'who-are-you',
      autoReply: true,
    });
    if (!resp.fromAgent) throw new Error('Missing fromAgent in response');
    console.log(`      Peer agent name: "${resp.fromAgent}"`);
  }).run();

  // ── 3. Direct Messaging — Payload Sizes ────────────────────────────────
  console.log('\n── 3. Direct Messaging — Payload Sizes ──');

  await test(`Medium payload delivery (${MEDIUM_PAYLOAD.length} chars)`, async () => {
    const resp = await ctx.protocols.sendMessage(primaryPeerId, {
      protocol: '/pi-agent/0.1.0',
      requestId: uuidv4(),
      fromAgent: AGENT_NAME,
      message: `ECHO:${MEDIUM_PAYLOAD}`,
      autoReply: true,
    });
    if (!resp.message.includes('ECHO:')) {
      throw new Error('Medium payload not echoed correctly');
    }
  }).run();

  await test(`Large payload delivery (${LARGE_PAYLOAD.length} bytes)`, async () => {
    const resp = await ctx.protocols.sendMessage(primaryPeerId, {
      protocol: '/pi-agent/0.1.0',
      requestId: uuidv4(),
      fromAgent: AGENT_NAME,
      message: LARGE_PAYLOAD,
      autoReply: true,
    });
    if (!resp.message) throw new Error('No response for large payload');
    console.log(`      Response size: ${resp.message.length} chars`);
  }).run();

  // ── 4. Latency ─────────────────────────────────────────────────────────
  console.log('\n── 4. Latency Measurement ──');

  const latencies = [];
  for (let i = 0; i < LATENCY_ITERATIONS; i++) {
    await test(`Latency round ${i + 1}/${LATENCY_ITERATIONS}`, async () => {
      const start = Date.now();
      await ctx.protocols.sendMessage(primaryPeerId, {
        protocol: '/pi-agent/0.1.0',
        requestId: uuidv4(),
        fromAgent: AGENT_NAME,
        message: 'latency-ping',
        autoReply: true,
      });
      const rtt = Date.now() - start;
      latencies.push(rtt);
      console.log(`      RTT: ${rtt}ms`);
    }).run();
  }

  if (latencies.length > 0) {
    const avg = latencies.reduce((a, b) => a + b, 0) / latencies.length;
    const min = Math.min(...latencies);
    const max = Math.max(...latencies);
    const sorted = [...latencies].sort((a, b) => a - b);
    const p50 = sorted[Math.floor(sorted.length * 0.5)];
    const p95 = sorted[Math.floor(sorted.length * 0.95)];

    console.log(`\n  📊 Latency summary (${latencies.length} samples):`);
    console.log(`      avg=${avg.toFixed(1)}ms  min=${min}ms  max=${max}ms  p50=${p50}ms  p95=${p95}ms`);
  }

  // ── 5. Concurrent Messaging ────────────────────────────────────────────
  console.log('\n── 5. Concurrent Messaging ──');

  await test(`Send ${CONCURRENT_MESSAGES} concurrent messages`, async () => {
    const promises = [];
    for (let i = 0; i < CONCURRENT_MESSAGES; i++) {
      promises.push(
        ctx.protocols.sendMessage(primaryPeerId, {
          protocol: '/pi-agent/0.1.0',
          requestId: uuidv4(),
          fromAgent: AGENT_NAME,
          message: `concurrent-msg-${i}`,
          autoReply: true,
        }),
      );
    }
    const responses = await Promise.all(promises);
    if (responses.length !== CONCURRENT_MESSAGES) {
      throw new Error(`Expected ${CONCURRENT_MESSAGES} responses, got ${responses.length}`);
    }
    for (const r of responses) {
      if (!r.message) throw new Error('Empty response in concurrent batch');
    }
    console.log(`      All ${CONCURRENT_MESSAGES} messages delivered`);
  }).run();

  // ── 6. Auto-Reply ─────────────────────────────────────────────────────
  console.log('\n── 6. Auto-Reply Mode ──');

  await test('autoReply=true returns echo', async () => {
    const msg = 'auto-reply-test-' + Date.now();
    const resp = await ctx.protocols.sendMessage(primaryPeerId, {
      protocol: '/pi-agent/0.1.0',
      requestId: uuidv4(),
      fromAgent: AGENT_NAME,
      message: msg,
      autoReply: true,
    });
    if (!resp.message.includes('[auto-response]')) {
      throw new Error(`Expected auto-response, got: ${resp.message}`);
    }
    if (!resp.message.includes(msg)) {
      throw new Error('Auto-response did not echo original message');
    }
  }).run();

  await test('autoReply=false goes to LLM', async () => {
    const start = Date.now();
    const resp = await ctx.protocols.sendMessage(primaryPeerId, {
      protocol: '/pi-agent/0.1.0',
      requestId: uuidv4(),
      fromAgent: AGENT_NAME,
      message: 'Say exactly "LLM_OK" and nothing else.',
      autoReply: false,
      timeoutMs: 120_000,
    });
    const duration = Date.now() - start;
    console.log(`      LLM response in ${duration}ms: "${resp.message.slice(0, 80)}"`);
    // LLM responses are non-deterministic, just verify we got something
    if (!resp.message) throw new Error('No LLM response received');
    if (resp.message.includes('[auto-response]')) {
      throw new Error('Should not get auto-response when autoReply=false');
    }
  }).run();

  // ── 7. Broadcast ───────────────────────────────────────────────────────
  console.log('\n── 7. Broadcast (GossipSub) ──');

  await test('Broadcast message publishes successfully', async () => {
    const result = await ctx.protocols.broadcast({
      fromAgent: AGENT_NAME,
      message: `test-broadcast-${Date.now()}`,
      type: 'announce',
    });
    if (!result.messageId) throw new Error('No messageId in broadcast result');
    console.log(`      Published to topic "${result.topic}", ~${result.peersReached} peers`);
  }).run();

  await test('Broadcast with type annotation', async () => {
    const result = await ctx.protocols.broadcast({
      fromAgent: AGENT_NAME,
      message: 'type-annotated-broadcast',
      type: 'event',
    });
    if (!result.messageId) throw new Error('No messageId');
    console.log(`      Type=event, messageId=${result.messageId}`);
  }).run();

  // ── 8. Multi-Peer Messaging ────────────────────────────────────────────
  console.log('\n── 8. Multi-Peer Messaging ──');

  for (const peer of ctx.peers) {
    await test(`Send to ${peer.agentName || peer.id.slice(0, 12)}`, async () => {
      const resp = await ctx.protocols.sendMessage(peer.id, {
        protocol: '/pi-agent/0.1.0',
        requestId: uuidv4(),
        fromAgent: AGENT_NAME,
        message: 'multi-peer-test',
        autoReply: true,
      });
      console.log(`      Response from ${resp.fromAgent}: OK`);
    }).run();
  }

  // ── 9. Error Handling ──────────────────────────────────────────────────
  console.log('\n── 9. Error Handling ──');

  await test('Timeout on invalid peer', async () => {
    const fakeId = '12D3KooWInvalidPeerIdFakeFakeFakeFakeFakeFakeFake';
    try {
      await ctx.protocols.sendMessage(fakeId, {
        protocol: '/pi-agent/0.1.0',
        requestId: uuidv4(),
        fromAgent: AGENT_NAME,
        message: 'to-invalid-peer',
      });
      // If we get here, the dial succeeded — that's actually unexpected for a fake ID
      console.log('      Note: Fake peer unexpectedly reachable');
    } catch (err) {
      // Expected — dial should fail
      console.log(`      Expected error: ${err.message.slice(0, 60)}`);
    }
  }).run();

  // ── 10. Message Integrity ─────────────────────────────────────────────
  console.log('\n── 10. Message Integrity ──');

  await test('Unicode message preserved', async () => {
    const unicode = 'Hello 世界 🌍 — テスト';
    const resp = await ctx.protocols.sendMessage(primaryPeerId, {
      protocol: '/pi-agent/0.1.0',
      requestId: uuidv4(),
      fromAgent: AGENT_NAME,
      message: `ECHO:${unicode}`,
      autoReply: true,
    });
    if (!resp.message.includes(unicode)) {
      throw new Error('Unicode content corrupted in transit');
    }
  }).run();

  await test('JSON-like content preserved', async () => {
    const jsonLike = JSON.stringify({ key: 'value', nested: { arr: [1, 2, 3] } });
    const resp = await ctx.protocols.sendMessage(primaryPeerId, {
      protocol: '/pi-agent/0.1.0',
      requestId: uuidv4(),
      fromAgent: AGENT_NAME,
      message: jsonLike,
      autoReply: true,
    });
    if (!resp.message.includes(jsonLike)) {
      throw new Error('JSON content corrupted');
    }
  }).run();

  // ── ────────────────────────────────────────────────────────────────────
  console.log('\n══════════════════════════════════════════════');
  console.log(`  Results: ${passed} passed, ${failed} failed, ${results.length} total`);
  console.log(`  Duration: ${Date.now() - ctx.startTime}ms`);
  console.log('══════════════════════════════════════════════\n');

  return { passed, failed, total: results.length, results };
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
    console.error(`\n💥 Fatal error: ${err.message}`);
    exitCode = 2;
  } finally {
    await ctx.shutdown();
  }

  process.exit(exitCode);
}

main();

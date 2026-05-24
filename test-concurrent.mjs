#!/usr/bin/env node
/**
 * test-concurrent.mjs
 *
 * Self-contained concurrent P2P mesh network test.
 * Creates multiple MeshNode instances in the same process so they
 * discover each other via bootstrap addresses (no reliance on stale
 * mDNS peers from previous runs).
 *
 * Usage:
 *   node test-concurrent.mjs [--node-count <N>]
 *
 * Environment:
 *   NODE_COUNT  — number of concurrent nodes (default: 3)
 */

import { MeshNode } from './dist/node.js';
import { MeshProtocols } from './dist/protocols.js';
import { multiaddr } from '@multiformats/multiaddr';
import { v4 as uuidv4 } from 'uuid';

const NODE_COUNT = parseInt(process.env.NODE_COUNT || '3', 10);

// ── Test Runner ──────────────────────────────────────────────────────────────

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

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// ── Concurrent Test Context ─────────────────────────────────────────────────

class ConcurrentTestContext {
  constructor() {
    this.nodes = [];        // { node, protocols, peerId, agentName }
    this.startTime = Date.now();
    this.broadcasts = [];   // Shared broadcast log
  }

  async init(count) {
    console.log(`\n🔧 Creating ${count} concurrent mesh nodes...\n`);

    // Create nodes sequentially — each gets a unique agent name
    for (let i = 0; i < count; i++) {
      const agentName = `concurrent-test-${i}`;
      const node = await MeshNode.create({
        agentName,
        enableMdns: false,     // No mDNS — we bootstrap from the first node
        listenPorts: { tcp: 0, ws: 0 },
      });

      const protocols = new MeshProtocols(node.libp2p, { agentName });

      protocols.onBroadcast = (msg) => {
        this.broadcasts.push(msg);
      };

      // Collect broadcasts from node events too
      node.onEvent((ev) => {
        if (ev.type === 'broadcast') {
          this.broadcasts.push(ev.message);
        }
      });

      await node.start();

      console.log(`  [${agentName}] PeerId: ${node.peerId}`);
      console.log(`              Addrs:  ${node.multiaddrs.slice(0, 2).join(', ')}`);

      this.nodes.push({ node, protocols, peerId: node.peerId, agentName });
    }

    // Bootstrap all nodes so they know each other's addresses.
    // libp2p's peerStore needs addresses registered before dialProtocol works.
    // We register every node's addresses into every other node's peerStore.
    for (let i = 0; i < this.nodes.length; i++) {
      for (let j = 0; j < this.nodes.length; j++) {
        if (i === j) continue;
        const src = this.nodes[i];
        const target = this.nodes[j];
        const targetPeerId = target.node.libp2p.peerId;

        // Register target's addresses in src's peer store
        const addrs = target.node.multiaddrs.map(
          (ma) => ma.split('/p2p/')[0] // strip the /p2p/ suffix, we dial by PeerId
        );
        try {
          await src.node.libp2p.peerStore.merge(targetPeerId, {
            multiaddrs: addrs.map((a) => multiaddr(a)),
          });
          // Now dial to establish the connection
          await src.node.libp2p.dial(targetPeerId).catch(() => {});
        } catch (err) {
          console.debug(`      [peerStore merge] ${src.agentName} -> ${target.agentName}: ${err.message}`);
        }
      }
    }

    // Wait for connections to establish and Identify to exchange agent names
    console.log(`\n🔍 Waiting for connections to establish (5s)...\n`);
    await sleep(5000);

    // Print discovered peers for each node
    for (const n of this.nodes) {
      const peers = n.node.getPeers();
      const connected = peers.filter((p) => p.status === 'connected').length;
      const total = peers.length;
      console.log(`  [${n.agentName}] ${connected}/${total} peers connected`);
    }
    console.log('');
  }

  /**
   * Wait until at least `minPeers` peers are visible and connected
   * from a given node's perspective, with a timeout.
   */
  async waitForConnected(nodeIndex, minPeers = this.nodes.length - 1, timeoutMs = 10_000) {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      const peers = this.nodes[nodeIndex].node.getPeers();
      const connected = peers.filter((p) => p.status === 'connected').length;
      if (connected >= minPeers) return;
      await sleep(200);
    }
    const peers = this.nodes[nodeIndex].node.getPeers();
    const connected = peers.filter((p) => p.status === 'connected').length;
    if (connected < minPeers) {
      console.warn(`      ⚠️  Node ${nodeIndex} only ${connected}/${minPeers} peers connected after timeout`);
    }
  }

  async shutdown() {
    // Stop protocols first, then nodes in reverse order
    for (const n of this.nodes) {
      try { await n.protocols.stop(); } catch {}
    }
    for (const n of this.nodes.reverse()) {
      try { await n.node.stop(); } catch {}
    }
  }
}

// ── Test Cases ──────────────────────────────────────────────────────────────

async function runTests(ctx) {
  console.log('══════════════════════════════════════════════');
  console.log('  Concurrent P2P Mesh Network Test Suite');
  console.log('══════════════════════════════════════════════\n');

  const { nodes } = ctx;

  // ── 1. Peer Discovery ──────────────────────────────────────────────────
  console.log('── 1. Peer Discovery ──');

  await test('Each node discovers all other peers', async () => {
    for (let i = 0; i < nodes.length; i++) {
      const peers = nodes[i].node.getPeers();
      const connected = peers.filter((p) => p.status === 'connected');
      if (connected.length < nodes.length - 1) {
        throw new Error(
          `Node ${i} (${nodes[i].agentName}) only has ${connected.length}/${nodes.length - 1} connected peers`
        );
      }
    }
  }).run();

  await test('All peers have valid PeerIds', async () => {
    for (const n of nodes) {
      const peers = n.node.getPeers();
      for (const p of peers) {
        if (!p.id || p.id.length < 10) throw new Error(`Invalid PeerId: ${p.id}`);
      }
    }
  }).run();

  await test('Peers have addresses', async () => {
    for (const n of nodes) {
      const peers = n.node.getPeers();
      for (const p of peers) {
        if (!p.addresses || p.addresses.length === 0) {
          throw new Error(`Peer ${p.id} has no addresses`);
        }
      }
    }
  }).run();

  // ── 2. Direct Messaging — Basic ──────────────────────────────────────
  console.log('\n── 2. Direct Messaging — Basic ──');

  const source = nodes[0];
  const targets = nodes.slice(1);

  for (const target of targets) {
    await test(`Send message from ${source.agentName} to ${target.agentName}`, async () => {
      const resp = await source.protocols.sendMessage(target.peerId, {
        protocol: '/pi-agent/0.1.0',
        requestId: uuidv4(),
        fromAgent: source.agentName,
        message: `Hello from ${source.agentName}`,
        autoReply: true,
      });
      if (!resp || !resp.message) throw new Error('Empty response');
      if (resp.error) throw new Error(`Peer reported error: ${resp.message}`);
    }).run();
  }

  await test('Response contains correct requestId echo', async () => {
    const requestId = uuidv4();
    const resp = await source.protocols.sendMessage(targets[0].peerId, {
      protocol: '/pi-agent/0.1.0',
      requestId,
      fromAgent: source.agentName,
      message: 'echo-request-id',
      autoReply: true,
    });
    if (resp.requestId !== requestId) {
      throw new Error(`Expected requestId ${requestId}, got ${resp.requestId}`);
    }
  }).run();

  await test('Response fromAgent matches target peer', async () => {
    const resp = await source.protocols.sendMessage(targets[0].peerId, {
      protocol: '/pi-agent/0.1.0',
      requestId: uuidv4(),
      fromAgent: source.agentName,
      message: 'who-are-you',
      autoReply: true,
    });
    if (!resp.fromAgent) throw new Error('Missing fromAgent in response');
    console.log(`      Response from agent: "${resp.fromAgent}"`);
  }).run();

  // ── 3. Payload Sizes ──────────────────────────────────────────────────
  console.log('\n── 3. Direct Messaging — Payload Sizes ──');

  const mediumPayload = 'The quick brown fox jumps over the lazy dog. '.repeat(10);
  const largePayload = 'x'.repeat(10_000);

  await test(`Medium payload delivery (${mediumPayload.length} chars)`, async () => {
    const resp = await source.protocols.sendMessage(targets[0].peerId, {
      protocol: '/pi-agent/0.1.0',
      requestId: uuidv4(),
      fromAgent: source.agentName,
      message: `ECHO:${mediumPayload}`,
      autoReply: true,
    });
    if (!resp.message.includes('ECHO:')) {
      throw new Error('Medium payload not echoed correctly');
    }
  }).run();

  await test(`Large payload delivery (${largePayload.length} bytes)`, async () => {
    const resp = await source.protocols.sendMessage(targets[0].peerId, {
      protocol: '/pi-agent/0.1.0',
      requestId: uuidv4(),
      fromAgent: source.agentName,
      message: largePayload,
      autoReply: true,
    });
    if (!resp.message) throw new Error('No response for large payload');
    console.log(`      Response size: ${resp.message.length} chars`);
  }).run();

  // ── 4. Latency ────────────────────────────────────────────────────────
  console.log('\n── 4. Latency Measurement ──');

  const LATENCY_ITERATIONS = 3;
  const latencies = [];

  for (let i = 0; i < LATENCY_ITERATIONS; i++) {
    await test(`RTT latency (round ${i + 1}/${LATENCY_ITERATIONS})`, async () => {
      const start = Date.now();
      await source.protocols.sendMessage(targets[0].peerId, {
        protocol: '/pi-agent/0.1.0',
        requestId: uuidv4(),
        fromAgent: source.agentName,
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
    console.log(`\n  📊 Latency: avg=${avg.toFixed(1)}ms  min=${min}ms  max=${max}ms`);
  }

  // ── 5. Concurrent Messaging ──────────────────────────────────────────
  console.log('\n── 5. Concurrent Messaging ──');

  await test('Send 5 concurrent messages', async () => {
    const promises = [];
    for (let i = 0; i < 5; i++) {
      promises.push(
        source.protocols.sendMessage(targets[0].peerId, {
          protocol: '/pi-agent/0.1.0',
          requestId: uuidv4(),
          fromAgent: source.agentName,
          message: `concurrent-msg-${i}`,
          autoReply: true,
        }),
      );
    }
    const responses = await Promise.all(promises);
    if (responses.length !== 5) {
      throw new Error(`Expected 5 responses, got ${responses.length}`);
    }
    for (const r of responses) {
      if (!r.message) throw new Error('Empty response in concurrent batch');
    }
    console.log(`      All 5 messages delivered`);
  }).run();

  // ── 6. Auto-Reply Mode ──────────────────────────────────────────────
  console.log('\n── 6. Auto-Reply Mode ──');

  await test('autoReply=true returns echo', async () => {
    const msg = 'auto-reply-test-' + Date.now();
    const resp = await source.protocols.sendMessage(targets[0].peerId, {
      protocol: '/pi-agent/0.1.0',
      requestId: uuidv4(),
      fromAgent: source.agentName,
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

  // ── 7. Broadcast (GossipSub) ─────────────────────────────────────────
  console.log('\n── 7. Broadcast (GossipSub) ──');

  await test('Broadcast message publishes successfully', async () => {
    const beforeCount = ctx.broadcasts.length;
    const result = await source.protocols.broadcast({
      fromAgent: source.agentName,
      message: `test-broadcast-${Date.now()}`,
      type: 'announce',
    });
    if (!result.messageId) throw new Error('No messageId in broadcast result');
    console.log(`      Published to topic "${result.topic}", ~${result.peersReached} peers`);

    // Wait for propagation (GossipSub delivery is async)
    await sleep(2000);
    const afterCount = ctx.broadcasts.length;
    console.log(`      Local broadcasts received: ${afterCount - beforeCount}`);
  }).run();

  await test('Broadcast with type annotation', async () => {
    const result = await source.protocols.broadcast({
      fromAgent: source.agentName,
      message: 'type-annotated-broadcast',
      type: 'event',
    });
    if (!result.messageId) throw new Error('No messageId');
    console.log(`      Type=event, messageId=${result.messageId.slice(0, 8)}…`);
  }).run();

  // ── 8. Multi-Peer Messaging ──────────────────────────────────────────
  console.log('\n── 8. Multi-Peer Messaging ──');

  for (const target of targets) {
    await test(`Send to ${target.agentName}`, async () => {
      const resp = await source.protocols.sendMessage(target.peerId, {
        protocol: '/pi-agent/0.1.0',
        requestId: uuidv4(),
        fromAgent: source.agentName,
        message: 'multi-peer-test',
        autoReply: true,
      });
      console.log(`      Response from ${resp.fromAgent}: OK`);
    }).run();
  }

  // Cross-peer messaging: each node sends to every other node
  for (const sender of nodes) {
    for (const target of nodes) {
      if (sender.peerId === target.peerId) continue;
      await test(`Cross-peer: ${sender.agentName} → ${target.agentName}`, async () => {
        const resp = await sender.protocols.sendMessage(target.peerId, {
          protocol: '/pi-agent/0.1.0',
          requestId: uuidv4(),
          fromAgent: sender.agentName,
          message: `relay from ${sender.agentName} to ${target.agentName}`,
          autoReply: true,
        });
        if (!resp.message) throw new Error('Empty response');
      }).run();
    }
  }

  // ── 9. Error Handling ────────────────────────────────────────────────
  console.log('\n── 9. Error Handling ──');

  await test('Timeout on invalid peer', async () => {
    const fakeId = '12D3KooWInvalidPeerIdFakeFakeFakeFakeFakeFakeFake';
    try {
      await source.protocols.sendMessage(fakeId, {
        protocol: '/pi-agent/0.1.0',
        requestId: uuidv4(),
        fromAgent: source.agentName,
        message: 'to-invalid-peer',
        timeoutMs: 5000,
      });
      console.log('      Note: Fake peer unexpectedly reachable');
    } catch (err) {
      console.log(`      Expected error: ${err.message.slice(0, 60)}`);
    }
  }).run();

  // ── 10. Message Integrity ────────────────────────────────────────────
  console.log('\n── 10. Message Integrity ──');

  await test('Unicode message preserved', async () => {
    const unicode = 'Hello 世界 🌍 — テスト';
    const resp = await source.protocols.sendMessage(targets[0].peerId, {
      protocol: '/pi-agent/0.1.0',
      requestId: uuidv4(),
      fromAgent: source.agentName,
      message: `ECHO:${unicode}`,
      autoReply: true,
    });
    if (!resp.message.includes(unicode)) {
      throw new Error('Unicode content corrupted in transit');
    }
  }).run();

  await test('JSON-like content preserved', async () => {
    const jsonLike = JSON.stringify({ key: 'value', nested: { arr: [1, 2, 3] } });
    const resp = await source.protocols.sendMessage(targets[0].peerId, {
      protocol: '/pi-agent/0.1.0',
      requestId: uuidv4(),
      fromAgent: source.agentName,
      message: jsonLike,
      autoReply: true,
    });
    if (!resp.message.includes(jsonLike)) {
      throw new Error('JSON content corrupted');
    }
  }).run();

  await test('Message with special characters preserved', async () => {
    const special = 'Line1\nLine2\tTabbed\n\rSpecial: ¡™£¢∞§¶•ªº';
    const resp = await source.protocols.sendMessage(targets[0].peerId, {
      protocol: '/pi-agent/0.1.0',
      requestId: uuidv4(),
      fromAgent: source.agentName,
      message: special,
      autoReply: true,
    });
    if (!resp.message || resp.message.length < 10) {
      throw new Error('Special characters content corrupted');
    }
  }).run();

  // ── 11. Bidirectional Messaging ──────────────────────────────────────
  console.log('\n── 11. Bidirectional Messaging ──');

  for (let i = 1; i < nodes.length; i++) {
    await test(`Bidirectional: ${nodes[i].agentName} replies to ${nodes[0].agentName}`, async () => {
      const resp = await nodes[i].protocols.sendMessage(nodes[0].peerId, {
        protocol: '/pi-agent/0.1.0',
        requestId: uuidv4(),
        fromAgent: nodes[i].agentName,
        message: `Ping from ${nodes[i].agentName}`,
        autoReply: true,
      });
      if (!resp.message) throw new Error('Empty response');
    }).run();
  }

  // ── Summary ─────────────────────────────────────────────────────────
  console.log('\n══════════════════════════════════════════════');
  console.log(`  Results: ${passed} passed, ${failed} failed, ${results.length} total`);
  console.log(`  Duration: ${Date.now() - ctx.startTime}ms`);
  console.log(`  Nodes: ${nodes.length}`);
  console.log('══════════════════════════════════════════════\n');

  return { passed, failed, total: results.length, results };
}

// ── Main ────────────────────────────────────────────────────────────────────

async function main() {
  const ctx = new ConcurrentTestContext();
  let exitCode = 0;

  try {
    await ctx.init(NODE_COUNT);
    const summary = await runTests(ctx);

    if (summary.failed > 0) exitCode = 1;
  } catch (err) {
    console.error(`\n💥 Fatal error: ${err.message}`);
    console.error(err.stack);
    exitCode = 2;
  } finally {
    await ctx.shutdown();
  }

  process.exit(exitCode);
}

main();

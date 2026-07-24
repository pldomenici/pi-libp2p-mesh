#!/usr/bin/env node
/**
 * pi-libp2p-mesh — Multi-Node Connectivity Test
 *
 * Spawns two MeshNode instances with distinct agent names and thoroughly
 * tests peer discovery, direct messaging, broadcast propagation, and
 * ChromaDB memory sharing.
 *
 * Usage:
 *   node test-multi-node.mjs
 */

import { MeshNode } from "./dist/node.js";
import { MeshProtocols } from "./dist/protocols.js";
import { AgentMemory, resolveMemoryConfig } from "./dist/memory.js";
import { ChromaDBLifecycle } from "./dist/chroma-lifecycle.js";
import { v4 as uuidv4 } from "uuid";

// ── Config ──────────────────────────────────────────────────────────────────

const TEST_TIMEOUT_MS = 90_000;
const DISCOVERY_TIMEOUT_MS = 20_000;
const MSG_TIMEOUT_MS = 15_000;
const BROADCAST_TIMEOUT_MS = 10_000;

// ── Test Runner ─────────────────────────────────────────────────────────────

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

// ── Helpers ─────────────────────────────────────────────────────────────────

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function waitFor(condition, timeoutMs, label) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (condition()) return;
    await sleep(300);
  }
  throw new Error(`Timeout waiting for: ${label}`);
}

// ── Test Context ────────────────────────────────────────────────────────────

class TestContext {
  constructor() {
    this.alpha = null;
    this.alphaProtocols = null;
    this.bravo = null;
    this.bravoProtocols = null;
    this.chromaLifecycle = null;
    this.agentMemory = null;

    this.alphaDiscoveredBravo = false;
    this.bravoDiscoveredAlpha = false;
    this.alphaReceivedMsg = null;
    this.bravoReceivedMsg = null;
    this.alphaReceivedBroadcast = null;
    this.bravoReceivedBroadcast = null;
  }

  async start() {
    console.log("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    console.log("  Multi-Node Mesh Connectivity Test");
    console.log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");

    // ── Start ChromaDB ──────────────────────────────────────────────
    console.log("🔧 Starting ChromaDB (if available)...");
    this.chromaLifecycle = new ChromaDBLifecycle({
      host: "localhost",
      port: 8000,
    });

    const chromaRunning = await this.chromaLifecycle.ensureRunning();
    if (chromaRunning) {
      console.log("   ChromaDB is running ✅");
    } else {
      console.log("   ChromaDB not available — memory tests will be skipped ⚠️");
    }

    // ── Create Node Alpha ───────────────────────────────────────────
    console.log('\n🔧 Creating node "alpha"...');
    this.alpha = await MeshNode.create(
      { agentName: "alpha", enableMdns: true, enableDht: false },
      "0.3.0",
    );
    this.alphaProtocols = new MeshProtocols(
      this.alpha.libp2p,
      { agentName: "alpha", gossipTopic: "pi-broadcast" },
      "0.3.0",
    );

    this.alpha.onEvent((ev) => {
      if (ev.type === "peer:discovered") {
        console.log(`   [alpha] Discovered: ${ev.peer.id.slice(0, 14)}...`);
      }
      if (ev.type === "peer:identified" && ev.agentName === "bravo") {
        this.alphaDiscoveredBravo = true;
        console.log(`   [alpha] Identified bravo: ${ev.peerId.slice(0, 14)}...`);
      }
      if (ev.type === "message") {
        this.alphaReceivedMsg = ev.request;
        console.log(`   [alpha] Message: "${ev.request.message.slice(0, 60)}..."`);
      }
      if (ev.type === "broadcast") {
        this.alphaReceivedBroadcast = ev.message;
        console.log(`   [alpha] Broadcast: "${ev.message.message.slice(0, 60)}..."`);
      }
    });

    await this.alpha.start();
    console.log(`   Alpha started: ${this.alpha.peerId}`);

    // ── Create Node Bravo ───────────────────────────────────────────
    console.log('\n🔧 Creating node "bravo"...');
    this.bravo = await MeshNode.create(
      { agentName: "bravo", enableMdns: true, enableDht: false },
      "0.3.0",
    );
    this.bravoProtocols = new MeshProtocols(
      this.bravo.libp2p,
      { agentName: "bravo", gossipTopic: "pi-broadcast" },
      "0.3.0",
    );

    this.bravo.onEvent((ev) => {
      if (ev.type === "peer:discovered") {
        console.log(`   [bravo] Discovered: ${ev.peer.id.slice(0, 14)}...`);
      }
      if (ev.type === "peer:identified" && ev.agentName === "alpha") {
        this.bravoDiscoveredAlpha = true;
        console.log(`   [bravo] Identified alpha: ${ev.peerId.slice(0, 14)}...`);
      }
      if (ev.type === "message") {
        this.bravoReceivedMsg = ev.request;
        console.log(`   [bravo] Message: "${ev.request.message.slice(0, 60)}..."`);
      }
      if (ev.type === "broadcast") {
        this.bravoReceivedBroadcast = ev.message;
        console.log(`   [bravo] Broadcast: "${ev.message.message.slice(0, 60)}..."`);
      }
    });

    await this.bravo.start();
    console.log(`   Bravo started: ${this.bravo.peerId}`);
  }

  async stop() {
    console.log("\n🛑 Shutting down nodes...");
    if (this.agentMemory) {
      await this.agentMemory.stop().catch(() => {});
    }
    if (this.bravoProtocols) await this.bravoProtocols.stop().catch(() => {});
    if (this.bravo) await this.bravo.stop().catch(() => {});
    if (this.alphaProtocols) await this.alphaProtocols.stop().catch(() => {});
    if (this.alpha) await this.alpha.stop().catch(() => {});
    if (this.chromaLifecycle) this.chromaLifecycle.stop();
    console.log("   Done.");
  }
}

// ── Main ────────────────────────────────────────────────────────────────────

(async () => {
  const ctx = new TestContext();

  try {
    await ctx.start();

    // ═══════════════════════════════════════════════════════════════════
    // PHASE 1: Peer Discovery
    // ═══════════════════════════════════════════════════════════════════

    console.log("\n── Phase 1: Peer Discovery ──");

    await test("Alpha discovers Bravo via mDNS", async () => {
      await waitFor(
        () => ctx.alphaDiscoveredBravo,
        DISCOVERY_TIMEOUT_MS,
        "alpha discovering bravo",
      );
    }).run();

    await test("Bravo discovers Alpha via mDNS", async () => {
      await waitFor(
        () => ctx.bravoDiscoveredAlpha,
        DISCOVERY_TIMEOUT_MS,
        "bravo discovering alpha",
      );
    }).run();

    await test("Both peers show as connected", async () => {
      await sleep(2000); // Let identify protocol settle

      const alphaPeers = ctx.alpha.libp2p.getPeers?.() ?? [];
      const bravoInAlpha = alphaPeers.some(
        (p) => p?.toString() === ctx.bravo.peerId,
      );
      if (!bravoInAlpha) throw new Error("Alpha does not see Bravo as connected");

      const bravoPeers = ctx.bravo.libp2p.getPeers?.() ?? [];
      const alphaInBravo = bravoPeers.some(
        (p) => p?.toString() === ctx.alpha.peerId,
      );
      if (!alphaInBravo) throw new Error("Bravo does not see Alpha as connected");

      console.log(`   Alpha peers: ${alphaPeers.length}, Bravo peers: ${bravoPeers.length}`);
    }).run();

    // Give GossipSub time to form topic meshes between newly connected peers
    console.log("\n   Waiting for GossipSub mesh formation (4s)...");
    await sleep(4000);

    // ═══════════════════════════════════════════════════════════════════
    // PHASE 2: Direct Messaging
    // ═══════════════════════════════════════════════════════════════════

    console.log("\n── Phase 2: Direct Messaging ──");

    await test("Alpha → Bravo direct message (sendMessage)", async () => {
      const message = `Hello from alpha at ${Date.now()}`;

      // Register request handler on bravo
      ctx.bravoProtocols.onRequest = async (peerId, request) => {
        ctx.bravoReceivedMsg = request;
        return `Bravo reply to: ${request.message}`;
      };

      const response = await ctx.alphaProtocols.sendMessage(
        ctx.bravo.peerId,
        {
          protocol: "/pi-agent/0.1.0",
          requestId: uuidv4(),
          fromAgent: "alpha",
          message,
        },
      );

      if (!response) throw new Error("No response received");
      if (!response.message.includes(message)) {
        throw new Error(`Unexpected response: ${response.message}`);
      }
      console.log(`   Sent → got: "${response.message.slice(0, 60)}..."`);
    }).run();

    await test("Bravo → Alpha direct message (sendMessage)", async () => {
      const message = `Hello from bravo at ${Date.now()}`;

      ctx.alphaProtocols.onRequest = async (peerId, request) => {
        ctx.alphaReceivedMsg = request;
        return `Alpha reply to: ${request.message}`;
      };

      const response = await ctx.bravoProtocols.sendMessage(
        ctx.alpha.peerId,
        {
          protocol: "/pi-agent/0.1.0",
          requestId: uuidv4(),
          fromAgent: "bravo",
          message,
        },
      );

      if (!response) throw new Error("No response received");
      if (!response.message.includes(message)) {
        throw new Error(`Unexpected response: ${response.message}`);
      }
      console.log(`   Sent → got: "${response.message.slice(0, 60)}..."`);
    }).run();

    await test("Alpha → Bravo large payload (10KB)", async () => {
      const largeMessage = "LARGE:" + "x".repeat(10_240);

      ctx.bravoProtocols.onRequest = async (_peerId, request) => {
        return `Got ${request.message.length} bytes`;
      };

      const response = await ctx.alphaProtocols.sendMessage(
        ctx.bravo.peerId,
        {
          protocol: "/pi-agent/0.1.0",
          requestId: uuidv4(),
          fromAgent: "alpha",
          message: largeMessage,
        },
      );

      if (!response) throw new Error("No response received");
      // "LARGE:" (6) + 10240 = 10246 chars
      const expectedBytes = 6 + 10_240;
      if (!response.message.includes(String(expectedBytes))) {
        throw new Error(`Payload size mismatch: got "${response.message}", expected ${expectedBytes} bytes`);
      }
      console.log(`   Sent ${expectedBytes} chars → got: "${response.message}"`);
    }).run();

    await test("Alpha → Bravo concurrent messages (5)", async () => {
      const count = 5;

      ctx.bravoProtocols.onRequest = async (_peerId, request) => {
        return `Reply to: ${request.message}`;
      };

      const promises = [];
      for (let i = 0; i < count; i++) {
        promises.push(
          ctx.alphaProtocols.sendMessage(ctx.bravo.peerId, {
            protocol: "/pi-agent/0.1.0",
            requestId: uuidv4(),
            fromAgent: "alpha",
            message: `concurrent-${i}`,
          }),
        );
      }

      const responses = await Promise.all(promises);
      const failures = responses.filter((r) => !r);
      if (failures.length > 0) throw new Error(`${failures.length} of ${count} messages failed`);
      console.log(`   All ${count} concurrent messages received responses`);
    }).run();

    // ═══════════════════════════════════════════════════════════════════
    // PHASE 3: Broadcast
    // ═══════════════════════════════════════════════════════════════════

    console.log("\n── Phase 3: Broadcast ──");

    // Check GossipSub mesh state before broadcasting
    const svcAlpha = ctx.alpha.libp2p.services;
    const svcBravo = ctx.bravo.libp2p.services;
    if (svcAlpha?.pubsub) {
      try {
        const subs = svcAlpha.pubsub.getSubscribers?.("pi-broadcast") ?? [];
        console.log(`   Alpha subscribers for "pi-broadcast": ${subs.length}`);
        console.log(`   Alpha topics: ${(svcAlpha.pubsub.getTopics?.() ?? []).join(", ") || "(none)"}`);
      } catch (e) { console.log(`   Alpha pubsub diag failed: ${e.message}`); }
    }
    if (svcBravo?.pubsub) {
      try {
        const subs = svcBravo.pubsub.getSubscribers?.("pi-broadcast") ?? [];
        console.log(`   Bravo subscribers for "pi-broadcast": ${subs.length}`);
        console.log(`   Bravo topics: ${(svcBravo.pubsub.getTopics?.() ?? []).join(", ") || "(none)"}`);
      } catch (e) { console.log(`   Bravo pubsub diag failed: ${e.message}`); }
    }

    await test("Alpha broadcasts — Bravo receives via GossipSub", async () => {
      const marker = `BCAST-ALPHA-${Date.now()}`;

      let bravoGotIt = null;
      const received = new Promise((resolve, reject) => {
        const timer = setTimeout(() => {
          if (!bravoGotIt) reject(new Error(`Bravo did not receive broadcast after ${BROADCAST_TIMEOUT_MS}ms`));
        }, BROADCAST_TIMEOUT_MS);

        ctx.bravoProtocols.onBroadcast = (msg) => {
          console.log(`   [bravo] GossipSub message on topic: ${msg.type}`);
          if (msg.message === marker) {
            bravoGotIt = msg;
            clearTimeout(timer);
            resolve(msg);
          }
        };
      });

      // Alpha broadcasts
      const result = await ctx.alphaProtocols.broadcast({
        fromAgent: "alpha",
        message: marker,
        type: "announce",
      });
      console.log(`   Alpha broadcast result: topic=${result.topic}, peersReached=${result.peersReached}`);

      await received;
      console.log(`   Broadcast "${marker.slice(0, 30)}..." received by Bravo ✅`);
    }).run();

    await test("Bravo broadcasts — Alpha receives via GossipSub", async () => {
      const marker = `BCAST-BRAVO-${Date.now()}`;

      let alphaGotIt = null;
      const received = new Promise((resolve, reject) => {
        const timer = setTimeout(() => {
          if (!alphaGotIt) reject(new Error("Alpha did not receive broadcast"));
        }, BROADCAST_TIMEOUT_MS);

        ctx.alphaProtocols.onBroadcast = (msg) => {
          if (msg.message === marker) {
            alphaGotIt = msg;
            clearTimeout(timer);
            resolve(msg);
          }
        };
      });

      await ctx.bravoProtocols.broadcast({
        fromAgent: "bravo",
        message: marker,
        type: "announce",
      });

      await received;
      console.log(`   Broadcast "${marker.slice(0, 30)}..." received by Alpha ✅`);
    }).run();

    // ═══════════════════════════════════════════════════════════════════
    // PHASE 4: ChromaDB Memory
    // ═══════════════════════════════════════════════════════════════════

    if (await ctx.chromaLifecycle.isRunning()) {
      console.log("\n── Phase 4: ChromaDB Memory ──");

      let memory = null;
      try {
        memory = await AgentMemory.create({
          host: "localhost",
          port: 8000,
          agentName: "test-runner",
        });
        console.log("   AgentMemory connected ✅");
      } catch (err) {
        console.log(`   AgentMemory connection failed: ${err.message} — skipping memory tests`);
      }

      if (memory) {
        ctx.agentMemory = memory;

        // Wait for embedding model warmup (background, non-blocking)
        await sleep(3000);

        await test("Memory store() + get() round-trip", async () => {
          const peerId = ctx.alpha.peerId;
          const key = "test-key";
          const value = `Test value ${Date.now()}`;

          await memory.store({ peerId, key, value });
          await sleep(1000);

          const entries = await memory.get(peerId, key);
          if (entries.length === 0) throw new Error("No entries returned");
          if (!entries.some((e) => e.value === value)) {
            throw new Error(`Value not found. Got: ${entries.map((e) => e.value.slice(0, 40)).join(" | ")}`);
          }
          console.log(`   Stored and retrieved: "${value}"`);
        }).run();

        await test("Memory semantic search", async () => {
          const peerId = ctx.bravo.peerId;
          await memory.store({
            peerId,
            key: "pref",
            value: "Prefers TypeScript over JavaScript for all projects and strongly types everything",
          });
          await memory.store({
            peerId,
            key: "pref",
            value: "Uses pnpm instead of npm for package management and monorepo tooling",
          });

          await sleep(1500);

          const results = await memory.search("TypeScript preference", {
            peerId,
            nResults: 3,
          });

          if (results.length === 0) throw new Error("No search results returned");
          if (results[0].distance > 0.8) throw new Error(`Distance too high: ${results[0].distance}`);
          console.log(`   Found ${results.length} results, top distance: ${results[0].distance.toFixed(4)}`);
        }).run();

        await test("Memory getKeys() counts entries", async () => {
          const keys = await memory.getKeys(ctx.bravo.peerId);
          const prefKey = keys.find((k) => k.key === "pref");
          if (!prefKey) throw new Error("Missing 'pref' key");
          if (prefKey.count < 2) throw new Error(`Expected >=2 entries for pref, got ${prefKey.count}`);
          console.log(`   Keys for bravo: ${keys.map((k) => `${k.key}(${k.count})`).join(", ")}`);
        }).run();

        await test("Memory deleteByPeer() cleans up", async () => {
          const countBefore = await memory.count(ctx.bravo.peerId);
          const deleted = await memory.deleteByPeer(ctx.bravo.peerId);
          const countAfter = await memory.count(ctx.bravo.peerId);

          if (deleted !== countBefore) {
            throw new Error(`Deleted ${deleted} but expected ${countBefore}`);
          }
          if (countAfter !== 0) throw new Error(`Still ${countAfter} entries after delete`);
          console.log(`   Deleted ${deleted} entries for bravo`);
        }).run();
      }
    } else {
      console.log("\n── Phase 4: ChromaDB Memory (SKIPPED — ChromaDB not available) ──");
    }

    // ═══════════════════════════════════════════════════════════════════
    // RESULTS
    // ═══════════════════════════════════════════════════════════════════

    console.log("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    console.log("  RESULTS");
    console.log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    for (const r of results) {
      const icon = r.status === "passed" ? "✅" : "❌";
      console.log(`  ${icon} ${r.name} (${r.duration}ms)`);
      if (r.error) console.log(`     Error: ${r.error}`);
    }

    console.log(`\n  ${passed} passed, ${failed} failed, ${results.length} total`);
    console.log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");

  } catch (err) {
    console.error("\n🔥 Fatal error:", err.message);
    console.error(err.stack);
  } finally {
    await ctx.stop();
    process.exit(failed > 0 ? 1 : 0);
  }
})();

/**
 * Mission Control — standalone observer node for the pi-libp2p-mesh network.
 *
 * Spins up a real libp2p node, joins the mesh, discovers peers via mDNS,
 * and streams live state to the 3D dashboard at http://localhost:9191.
 *
 * Usage: node start-mission-control.mjs [port]
 */

import { createLibp2p } from "libp2p";
import { tcp } from "@libp2p/tcp";
import { webSockets } from "@libp2p/websockets";
import { noise } from "@chainsafe/libp2p-noise";
import { yamux } from "@chainsafe/libp2p-yamux";
import { mdns } from "@libp2p/mdns";
import { identify } from "@libp2p/identify";
import { gossipsub } from "@libp2p/gossipsub";
import { generateKeyPair } from "@libp2p/crypto/keys";
import { peerIdFromString } from "@libp2p/peer-id";
import { preSharedKey } from "@libp2p/pnet";
import { readFile } from "node:fs/promises";
import { existsSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";

import { MissionControlServer } from "./dist/mission-control-server.js";

// ── Config ────────────────────────────────────────────────────────────────

const PORT = parseInt(process.argv[2], 10) || 9191;
const AGENT_NAME = `mc-${process.env.HOSTNAME || process.env.HOST || "observer"}`;

// ── Shared state (populated at startup) ───────────────────────────────────

let mcs /** @type {MissionControlServer} */;
const peerMap = new Map();
const commLinksMap = new Map();
let selfPeerId = "";
let selfAddresses = [];
let messagesSent = 0;
let messagesReceived = 0;
let broadcastsSent = 0;
let broadcastsReceived = 0;

// ── Auto-dial discovered peers ───────────────────────────────────────────

function setupAutoDial(node, name) {
  const pendingDials = new Set();
  let dialTimer = null;
  const DIAL_DEBOUNCE_MS = 200;

  function flush() {
    const peers = [...pendingDials];
    pendingDials.clear();
    for (const id of peers) {
      try {
        const connections = node.getConnections(id);
        if (connections.length > 0) continue; // already connected
        node.dial(peerIdFromString(id)).catch((err) => {
          console.debug(`[${name}] auto-dial failed for ${id.slice(-8)}: ${err.message}`);
        });
      } catch (err) {
        console.debug(`[${name}] dial error for ${id.slice(-8)}: ${err.message}`);
      }
    }
  }

  node.addEventListener("peer:discovery", (evt) => {
    const detail = evt.detail;
    if (!detail?.id) return;
    const id = detail.id.toString();
    if (id === node.peerId.toString()) return; // skip self

    pendingDials.add(id);
    if (!dialTimer) {
      dialTimer = setTimeout(() => {
        dialTimer = null;
        flush();
      }, DIAL_DEBOUNCE_MS);
    }
  });
}

// ── Helpers ───────────────────────────────────────────────────────────────

async function loadPsk() {
  const paths = [
    join(homedir(), ".pi", "mesh-psk.txt"),
    join(process.cwd(), "mesh-psk.txt"),
  ];
  for (const p of paths) {
    if (existsSync(p)) {
      const raw = (await readFile(p, "utf-8")).trim();
      if (raw) {
        console.log(`[mc] Loaded PSK from ${p}`);
        return raw;
      }
    }
  }
  return null;
}

function commKey(a, b) {
  return a < b ? `${a}::${b}` : `${b}::${a}`;
}

function trackMessage(from, to, rttMs) {
  const key = commKey(from, to);
  const entry = commLinksMap.get(key);
  if (entry) {
    entry.count++;
    entry.totalRtt += rttMs || 0;
    entry.avgRttMs = Math.round(entry.totalRtt / entry.count);
    entry.lastTimestamp = Date.now();
  } else {
    commLinksMap.set(key, {
      from,
      to,
      count: 1,
      totalRtt: rttMs || 0,
      avgRttMs: rttMs || 0,
      lastTimestamp: Date.now(),
    });
  }
}

function buildState() {
  const peers = [];
  for (const [id, p] of peerMap) {
    peers.push({
      peerId: id,
      agentName: p.agentName,
      status: p.status,
      addresses: p.addresses || [],
      discoveredAt: p.discoveredAt,
      disconnectedAt: p.disconnectedAt,
      messagesTo: p.messagesTo || 0,
      messagesFrom: p.messagesFrom || 0,
      role: p.role,
    });
  }

  const connectedCount = peers.filter((p) => p.status === "connected").length;

  return {
    self: {
      peerId: selfPeerId,
      agentName: AGENT_NAME,
      addresses: selfAddresses,
    },
    peers,
    stats: {
      totalPeers: peers.length,
      connectedPeers: connectedCount,
      messagesSent,
      messagesReceived,
      broadcastsSent,
      broadcastsReceived,
      messagesPerSec: 0,
      pendingQueueDepth: 0,
      errors: 0,
    },
    commLinks: [...commLinksMap.values()],
  };
}

// ── Create libp2p node ────────────────────────────────────────────────────

async function createNode() {
  const privateKey = await generateKeyPair("Ed25519");
  const psk = await loadPsk();

  const modules /** @type {any} */ = {
    addresses: { listen: ["/ip4/0.0.0.0/tcp/0"] },
    transports: [tcp(), webSockets()],
    connectionEncrypters: [noise()],
    streamMuxers: [yamux()],
    nodeInfo: {
      userAgent: `pi-libp2p-mesh/1.0.0/${AGENT_NAME}`,
      name: "pi-libp2p-mesh",
      version: `1.0.0/${AGENT_NAME}`,
    },
    services: {
      identify: identify(),
      pubsub: gossipsub(),
      mdns: mdns(),
    },
    privateKey,
    connectionManager: {
      maxConnections: 100,
      minConnections: 5,
    },
  };

  if (psk) {
    modules.connectionProtector = preSharedKey({ psk: new TextEncoder().encode(psk) });
  }

  return createLibp2p(modules);
}

// ── Main ──────────────────────────────────────────────────────────────────

async function main() {
  console.log("[mc] Starting libp2p node...");
  const node = await createNode();

  // ── Start Mission Control server ─────────────────────────────────────
  mcs = new MissionControlServer({ port: PORT });
  await mcs.start();

  // ── Peer discovery via Identify ─────────────────────────────────────
  node.addEventListener("peer:identify", (evt) => {
    const result /** @type {IdentifyResult} */ = evt.detail;
    const id = result.peerId.toString();
    const existing = peerMap.get(id);

    if (!existing || existing.status !== "connected") {
      console.log(`[mc] Discovered: ${id} (${result.agentVersion || "unknown"})`);
    }

    // Extract human-readable agent name from the agentVersion string.
    // Formats:
    //   pi-libp2p-mesh/<version>/<agentName>  →  <agentName>
    //   pi-worker-<name>                      →  worker-<name>
    //   pi-<anything>                         →  <anything>
    let agentName;
    const av = result.agentVersion || "";
    const libp2pPrefix = "pi-libp2p-mesh/";
    if (av.startsWith(libp2pPrefix)) {
      const parts = av.slice(libp2pPrefix.length).split("/");
      agentName = parts.length >= 2 ? parts.slice(1).join("/") : parts[0];
    } else {
      agentName = av.replace(/^pi-/, "") || id.slice(-8);
    }

    peerMap.set(id, {
      agentName,
      status: "connected",
      addresses: (result.listenAddrs || []).map((m) => m.toString()),
      discoveredAt: existing?.discoveredAt || Date.now(),
      disconnectedAt: undefined,
      messagesTo: existing?.messagesTo || 0,
      messagesFrom: existing?.messagesFrom || 0,
      role: "peer",
    });
  });

  // ── Connection tracking ─────────────────────────────────────────────
  node.addEventListener("peer:connect", (evt) => {
    const id = evt.detail.toString();
    const p = peerMap.get(id);
    if (p) {
      p.status = "connected";
      p.disconnectedAt = undefined;
      p.discoveredAt = p.discoveredAt || Date.now();
    }
  });

  node.addEventListener("peer:disconnect", (evt) => {
    const id = evt.detail.toString();
    const p = peerMap.get(id);
    if (p) {
      p.status = "disconnected";
      p.disconnectedAt = Date.now();
    }
  });

  // ── GossipSub listener ──────────────────────────────────────────────
  node.services.pubsub.addEventListener("message", (evt) => {
    broadcastsReceived++;
    try {
      const fromPeerId = evt.detail.from?.toString() || "unknown";
      const msg = new TextDecoder().decode(evt.detail.data);
      const fromName = (peerMap.get(fromPeerId) || {}).agentName || fromPeerId.slice(-8);

      mcs.emitBroadcast({
        from: fromPeerId,
        fromName,
        message: msg.length > 200 ? msg.slice(0, 200) + "..." : msg,
        type: evt.detail.topic || undefined,
        timestamp: Date.now(),
      });
    } catch { /* ignore malformed messages */ }
  });

  // ── Subscribe to topics ─────────────────────────────────────────────
  node.services.pubsub.subscribe("pi-broadcast");
  node.services.pubsub.subscribe("pi-memory-host");

  await node.start();

  selfPeerId = node.peerId.toString();
  selfAddresses = node.getMultiaddrs().map((m) => m.toString());

  setupAutoDial(node, "mc");

  console.log(`[mc] Node online: ${selfPeerId}`);
  console.log(`[mc] Listening on: ${selfAddresses.join(", ")}`);

  // Push initial state immediately so the dashboard shows the self node
  mcs.updateState(buildState());

  // Periodic state refresh: every 2s
  setInterval(() => {
    mcs.updateState(buildState());
  }, 2_000);

  // Prune peers disconnected for >60s: every 10s
  setInterval(() => {
    const cutoff = Date.now() - 60_000;
    for (const [id, p] of peerMap) {
      if (p.status === "disconnected" && p.disconnectedAt && p.disconnectedAt < cutoff) {
        peerMap.delete(id);
        console.log(`[mc] Pruned stale peer: ${p.agentName || id.slice(-8)}`);
      }
    }
  }, 10_000);

  console.log(`\n🛰  Mission Control: http://localhost:${PORT}\n`);
  console.log(`   Observer node: ${selfPeerId}`);
  console.log(`   Press Ctrl+C to stop.\n`);
}

main().catch((err) => {
  console.error("[mc] Fatal error:", err);
  process.exit(1);
});

// Graceful shutdown
process.on("SIGINT", () => {
  console.log("\n[mc] Shutting down...");
  process.exit(0);
});

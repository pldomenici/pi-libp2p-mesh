/**
 * Mesh worker node — lightweight libp2p peer for the pi mesh network.
 *
 * Spins up a libp2p node, joins the mesh via mDNS, handles direct messages
 * with auto-reply, and subscribes to GossipSub. No LLM required.
 *
 * Usage: node worker.mjs <name>
 * Example: node worker.mjs alpha
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
import * as cborg from "cborg";
import { readFile } from "node:fs/promises";
import { existsSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";

// ── Config ────────────────────────────────────────────────────────────────

const WORKER_NAME = process.argv[2];
if (!WORKER_NAME) {
  console.error("Usage: node worker.mjs <name>");
  process.exit(1);
}

const AGENT_NAME = `worker-${WORKER_NAME}`;
const PROTOCOL = "/pi-agent/0.1.0";

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
        console.log(`[${AGENT_NAME}] Loaded PSK from ${p}`);
        return raw;
      }
    }
  }
  return null;
}

/** Read all chunks from a libp2p stream into a single Uint8Array. */
async function readStream(stream) {
  const chunks = [];
  for await (const raw of stream) {
    chunks.push(raw instanceof Uint8Array ? raw : raw.subarray());
  }
  const total = chunks.reduce((s, c) => s + c.length, 0);
  const merged = new Uint8Array(total);
  let offset = 0;
  for (const c of chunks) {
    merged.set(c, offset);
    offset += c.length;
  }
  return merged;
}

// ── Create libp2p node ────────────────────────────────────────────────────

async function createNode() {
  const privateKey = await generateKeyPair("Ed25519");
  const psk = await loadPsk();

  const opts = {
    addresses: { listen: ["/ip4/0.0.0.0/tcp/0"] },
    transports: [tcp(), webSockets()],
    connectionEncrypters: [noise()],
    streamMuxers: [yamux()],
    nodeInfo: { userAgent: `pi-${AGENT_NAME}` },
    services: {
      identify: identify(),
      pubsub: gossipsub(),
      mdns: mdns(),
    },
    privateKey,
    connectionManager: { maxConnections: 50, minConnections: 1 },
  };

  if (psk) {
    opts.connectionProtector = (await import("@libp2p/pnet")).preSharedKey({
      psk: new TextEncoder().encode(psk),
    });
  }

  return createLibp2p(opts);
}

// ── Protocol handler ──────────────────────────────────────────────────────

function setupProtocolHandler(node, name, peerId) {
  node.handle(PROTOCOL, async (stream, connection) => {
    const remotePeer = connection.remotePeer.toString();
    try {
      const raw = await readStream(stream);
      const request = cborg.decode(raw);

      console.log(`[${name}] ← message from ${request.fromAgent || remotePeer}: "${request.message}"`);

      const response = {
        requestId: request.requestId,
        fromAgent: name,
        fromPeerId: peerId,
        timestamp: Date.now(),
        message: `[${name}] Received: "${request.message}"`,
        error: false,
      };
      await stream.send(cborg.encode(response));
      console.log(`[${name}] → reply sent`);
    } catch (err) {
      console.error(`[${name}] handler error:`, err.message);
    }
  });
}

// ── Auto-dial discovered peers ───────────────────────────────────────────

function setupAutoDial(node, name) {
  const pendingDials = new Set();
  let dialTimer = null;
  const DIAL_DEBOUNCE_MS = 200;

  function flush() {
    const peers = [...pendingDials];
    pendingDials.clear();
    for (const id of peers) {
      const connections = node.getConnections(id);
      if (connections.length > 0) continue; // already connected
      try {
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

// ── GossipSub listener ────────────────────────────────────────────────────

function setupGossipListener(node, name) {
  node.services.pubsub.addEventListener("message", (evt) => {
    try {
      const msg = new TextDecoder().decode(evt.detail.data);
      const from = evt.detail.from?.toString() || "?";
      console.log(`[${name}] 📢 broadcast from ${from.slice(-8)}: ${msg.slice(0, 120)}`);
    } catch {}
  });

  node.services.pubsub.subscribe("pi-broadcast");
  node.services.pubsub.subscribe("pi-memory-host");
}

// ── Main ──────────────────────────────────────────────────────────────────

async function main() {
  console.log(`[${AGENT_NAME}] Starting...`);
  const node = await createNode();

  await node.start();

  const peerId = node.peerId.toString();
  const addrs = node.getMultiaddrs().map((m) => m.toString());

  // Register protocol *after* start
  setupProtocolHandler(node, AGENT_NAME, peerId);
  setupGossipListener(node, AGENT_NAME);
  setupAutoDial(node, AGENT_NAME);

  // Track discovered peers
  node.addEventListener("peer:identify", (evt) => {
    const id = evt.detail.peerId.toString();
    const ver = evt.detail.agentVersion || "?";
    console.log(`[${AGENT_NAME}] 👁 discovered: ${id.slice(-8)} (${ver})`);
  });

  console.log(`[${AGENT_NAME}] Online — ${peerId}`);
  console.log(`[${AGENT_NAME}] Listening: ${addrs[0]}`);
  console.log("");

  // Stay alive
  process.on("SIGINT", () => {
    console.log(`\n[${AGENT_NAME}] Shutting down...`);
    node.stop().then(() => process.exit(0));
  });
  process.on("SIGTERM", () => {
    node.stop().then(() => process.exit(0));
  });
}

main().catch((err) => {
  console.error(`[${AGENT_NAME}] Fatal:`, err);
  process.exit(1);
});

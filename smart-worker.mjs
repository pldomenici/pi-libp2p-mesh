/**
 * Smart worker v2 — libp2p mesh peer backed by Gemini API directly.
 *
 * Creates a libp2p node for mesh connectivity. When a direct message arrives
 * on /pi-agent/0.1.0, calls the Gemini API (or DeepSeek) for an LLM response.
 * No pi --print needed — avoids conflicting libp2p nodes.
 *
 * Usage: node smart-worker.mjs <name> [model]
 * Example: node smart-worker.mjs Paul
 *          node smart-worker.mjs Paul deepseek
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
  console.error("Usage: node smart-worker.mjs <name> [model]");
  console.error("  model: gemini (default) | deepseek");
  process.exit(1);
}

const MODEL_CHOICE = process.argv[3] || "gemini";
const AGENT_NAME = `agent-${WORKER_NAME}`;
const PROTOCOL = "/pi-agent/0.1.0";

const GEMINI_API_KEY = process.env.GEMINI_API_KEY || "";
const DEEPSEEK_API_KEY = process.env.DEEPSEEK_API_KEY || "";

const SYSTEM_PROMPT = `You are ${WORKER_NAME}, a world-class coding wizard and software architect connected via a P2P mesh network. You have deep expertise in all programming languages, frameworks, system design, algorithms, and debugging. You respond to queries with concise, accurate, and helpful answers. When asked to write code, you provide clean, well-commented solutions. When asked to explain concepts, you are thorough but brief. You are a mesh peer — keep responses focused and avoid unnecessary preamble.`;

// ── Helpers ───────────────────────────────────────────────────────────────

async function loadPsk() {
  const paths = [
    join(homedir(), ".pi", "mesh-psk.txt"),
    join(process.cwd(), "mesh-psk.txt"),
  ];
  for (const p of paths) {
    if (existsSync(p)) {
      const raw = (await readFile(p, "utf-8")).trim();
      if (raw) return raw;
    }
  }
  return null;
}

async function readStream(stream) {
  const chunks = [];
  for await (const raw of stream) {
    chunks.push(raw instanceof Uint8Array ? raw : raw.subarray());
  }
  const total = chunks.reduce((s, c) => s + c.length, 0);
  const merged = new Uint8Array(total);
  let offset = 0;
  for (const c of chunks) { merged.set(c, offset); offset += c.length; }
  return merged;
}

// ── LLM backends ──────────────────────────────────────────────────────────

async function askGemini(query) {
  const url = `https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key=${GEMINI_API_KEY}`;
  const body = {
    system_instruction: { parts: [{ text: SYSTEM_PROMPT }] },
    contents: [{ role: "user", parts: [{ text: query }] }],
    generationConfig: { temperature: 0.7, maxOutputTokens: 2048 },
  };

  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(60_000),
  });

  if (!res.ok) {
    const err = await res.text();
    return `[Gemini error ${res.status}] ${err.slice(0, 500)}`;
  }

  const data = await res.json();
  return data.candidates?.[0]?.content?.parts?.[0]?.text || "(empty response)";
}

async function askDeepSeek(query) {
  const url = "https://api.deepseek.com/v1/chat/completions";
  const body = {
    model: "deepseek-v4-pro",
    messages: [
      { role: "system", content: SYSTEM_PROMPT },
      { role: "user", content: query },
    ],
    temperature: 0.7,
    max_tokens: 2048,
  };

  const res = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Authorization": `Bearer ${DEEPSEEK_API_KEY}`,
    },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(60_000),
  });

  if (!res.ok) {
    const err = await res.text();
    return `[DeepSeek error ${res.status}] ${err.slice(0, 500)}`;
  }

  const data = await res.json();
  return data.choices?.[0]?.message?.content || "(empty response)";
}

async function askLLM(query) {
  if (MODEL_CHOICE === "deepseek" && DEEPSEEK_API_KEY) {
    return askDeepSeek(query);
  }
  if (GEMINI_API_KEY) {
    return askGemini(query);
  }
  return `[${AGENT_NAME}] No API key configured. Set GEMINI_API_KEY or DEEPSEEK_API_KEY.`;
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
      const query = request.message || "";

      let answer;
      if (request.autoReply === true) {
        answer = `[auto-response] Received: "${query.slice(0, 200)}"`;
        console.log(`[${name}] ← auto-reply to ${request.fromAgent || remotePeer.slice(-8)}: "${query.slice(0, 100)}"`);
      } else {
        console.log(`[${name}] ← query from ${request.fromAgent || remotePeer.slice(-8)}: "${query.slice(0, 100)}"`);
        answer = await askLLM(query);
      }
      console.log(`[${name}] → response (${answer.length} chars)`);

      const response = {
        requestId: request.requestId,
        fromAgent: name,
        fromPeerId: peerId,
        timestamp: Date.now(),
        message: answer,
        error: false,
      };
      await stream.send(cborg.encode(response));
      await stream.close();
    } catch (err) {
      console.error(`[${name}] handler error:`, err.message);
    }
  });
}

// ── Auto-dial ─────────────────────────────────────────────────────────────

function setupAutoDial(node, name) {
  const pendingDials = new Set();
  let dialTimer = null;

  function flush() {
    const peers = [...pendingDials];
    pendingDials.clear();
    for (const id of peers) {
      if (node.getConnections(id).length > 0) continue;
      try {
        node.dial(peerIdFromString(id)).catch(() => {});
      } catch {}
    }
  }

  node.addEventListener("peer:discovery", (evt) => {
    const detail = evt.detail;
    if (!detail?.id) return;
    const id = detail.id.toString();
    if (id === node.peerId.toString()) return;
    pendingDials.add(id);
    if (!dialTimer) dialTimer = setTimeout(() => { dialTimer = null; flush(); }, 200);
  });
}

// ── GossipSub ─────────────────────────────────────────────────────────────

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
  console.log(`[${AGENT_NAME}] Starting (${MODEL_CHOICE} backend)...`);
  const node = await createNode();
  await node.start();

  const peerId = node.peerId.toString();
  const addrs = node.getMultiaddrs().map((m) => m.toString());

  setupProtocolHandler(node, AGENT_NAME, peerId);
  setupGossipListener(node, AGENT_NAME);
  setupAutoDial(node, AGENT_NAME);

  node.addEventListener("peer:identify", (evt) => {
    const id = evt.detail.peerId.toString();
    const ver = evt.detail.agentVersion || "?";
    console.log(`[${AGENT_NAME}] 👁 discovered: ${id.slice(-8)} (${ver})`);
  });

  console.log(`[${AGENT_NAME}] Online — ${peerId}`);
  console.log(`[${AGENT_NAME}] Listening: ${addrs[0]}`);
  console.log(`[${AGENT_NAME}] LLM backend: ${MODEL_CHOICE}`);
  console.log("");

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

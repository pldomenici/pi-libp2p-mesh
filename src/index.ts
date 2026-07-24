/**
 * pi-libp2p-mesh — P2P mesh network extension for pi agents.
 *
 * Provides a libp2p-based overlay network so pi agents can discover each other,
 * send direct messages, and broadcast across the swarm. Exposes four tools to
 * the LLM so it can orchestrate multi-agent workflows.
 *
 * Architecture:
 *   src/types.ts        — shared types and interfaces
 *   src/node.ts         — libp2p node factory (transport, crypto, muxer, discovery)
 *   src/protocols.ts    — custom protocol handler (/pi-agent/0.1.0) + GossipSub control
 *   src/tools.ts        — pi extension tool definitions (mesh_list_peers, etc.)
 *   src/index.ts        — this file: extension entry point, lifecycle wiring
 *
 * Lifecycle:
 *   session_start → create libp2p node, start listening
 *   session_shutdown → stop libp2p node, clean up
 *
 * Configuration:
 *   The mesh picks up the agent name from the pi config (--agent-name flag,
 *   PI_MESH_NAME env var, or defaults to pi-<hostname>).
 */

import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import type { MeshConfig, MeshNodeEvent } from "./types.js";
import { MeshNode } from "./node.js";
import { MeshProtocols } from "./protocols.js";
import { registerMeshTools, registerMemoryTools, setMeshProtocols, setAgentMemory, listPeers, pruneAllDisconnected, pruneStalePeers, recordBroadcast, cleanupStaleRtt, RTT_CLEANUP_INTERVAL_MS, type MeshStore } from "./tools.js";
import { AgentMemory, resolveMemoryConfig } from "./memory.js";
import { ChromaDBLifecycle } from "./chroma-lifecycle.js";
import { MissionControlServer } from "./mission-control-server.js";
import type { MeshState, MessageFlow, BroadcastFlow } from "./mission-control-server.js";
import os from "node:os";

/** Extension version — reported via Identify protocol for stale-build detection. */
export const EXTENSION_VERSION = "0.3.0";

// ── Shared State ─────────────────────────────────────────────────────────────
// This singleton is re-created on each extension load (session reload).
// Peer state is re-discovered on each session start.

let meshNode: MeshNode | null = null;
let meshProtocols: MeshProtocols | null = null;
let agentMemory: AgentMemory | null = null;
let chromaLifecycle: ChromaDBLifecycle | null = null;
let missionControl: MissionControlServer | null = null;
let missionControlInterval: ReturnType<typeof setInterval> | null = null;
let memoryHostAnnounceInterval: ReturnType<typeof setInterval> | null = null;
let pruneInterval: ReturnType<typeof setInterval> | null = null;
let rttCleanupInterval: ReturnType<typeof setInterval> | null = null;

/** Guard flag: set true during session_start, false during session_shutdown.
 * Prevents duplicate event handlers from processing events after reload. */
let sessionActive = false;

/** Counter: incremented before sendUserMessage, decremented in agent_end.
 * Prevents user chat agent_end events from stealing mesh request resolvers. */
let pendingMeshTurns = 0;

const store: MeshStore = {
  peers: new Map(),
  broadcastHistory: [],
  agentName: "", // set during extension init after flag is read
  peerId: "",    // set after mesh node starts
  autoReplyAll: false, // when true, all incoming messages auto-reply without LLM
};

// Mission-control stats (cumulative counters)
const mcStats = {
  messagesSent: 0,
  messagesReceived: 0,
  broadcastsSent: 0,
  broadcastsReceived: 0,
  errors: 0,
  timeouts: 0,
  /** Track per-peer message counts: peerId → {sent, received} */
  peerMessages: new Map<string, { sent: number; received: number }>(),
  /** Communication links: "fromId→toId" → { count, lastTimestamp, totalRtt, rttSamples } */
  commLinks: new Map<string, { count: number; lastTimestamp: number; totalRtt: number; rttSamples: number }>(),
  /** Timestamps of recent messages for rate calculation */
  recentMsgTimestamps: [] as number[],
  /** Pending RTT measurements: requestId → sendTimestamp */
  pendingRtt: new Map<string, number>(),
};

/** A pending LLM request awaiting agent_end resolution. */
interface PendingResolver {
  resolve: (text: string) => void;
  timer: ReturnType<typeof setTimeout>;
  /** True once max heartbeats reached and timeout resolved. */
  resolved: boolean;
  peerId: string;
  requestId: string;
  messagePreview: string;
}

/**
 * FIFO queue of resolvers for agent_end events.
 * Each `sendUserMessage` call pushes a resolver; each `agent_end` pops one.
 * This provides reliable 1:1 pairing regardless of interleaving with
 * user chat or broadcast-forward turns.
 */
const pendingResolvers: PendingResolver[] = [];

// ── Helpers ──────────────────────────────────────────────────────────────────

function notify(_pi: ExtensionAPI, msg: string, level: "info" | "warning" | "error" = "info") {
  console.log(`[pi-libp2p-mesh] ${level}: ${msg}`);
}

/** Parse a string flag value as integer, returning undefined if empty/zero. */
function parseOptionalInt(val: unknown): number | undefined {
  if (val === undefined || val === null || val === "" || val === 0) return undefined;
  const n = Number(val);
  return Number.isNaN(n) ? undefined : n;
}

/** Parse a string flag value as float, returning undefined if empty/zero. */
function parseOptionalFloat(val: unknown): number | undefined {
  if (val === undefined || val === null || val === "" || val === 0) return undefined;
  const n = Number(val);
  return Number.isNaN(n) ? undefined : n;
}

/** Extract the first non-loopback IPv4 address from the libp2p node's multiaddrs. */
function resolveLocalIp(libp2p: any): string | null {
  try {
    const addrs: string[] = libp2p.getMultiaddrs?.()?.map((m: any) => m.toString()) ?? [];
    for (const addr of addrs) {
      const m = addr.match(/\/ip4\/(\d+\.\d+\.\d+\.\d+)/);
      if (m && m[1] !== "127.0.0.1") return m[1];
    }
  } catch { /* ignore */ }
  return null;
}

/** Role registry — maps agent names to their assigned roles. */
const AGENT_ROLES: Record<string, string> = {
  'alpha': 'Organizer',
  'pi-fedora-laptop': 'Director',
  'bravo': 'Network Monitor',
  'charlie': 'Memory Curator',
  'delta': 'QA Engineer',
  'blair': 'Developer',
};

/** Build a full MeshState snapshot for the mission-control dashboard. */
function buildMissionControlState(): MeshState {
  const peerList = [];
  for (const [id, p] of store.peers) {
    if (id === store.peerId) continue; // skip self
    const pm = mcStats.peerMessages.get(id);
    peerList.push({
      peerId: id,
      agentName: p.agentName,
      status: p.status,
      addresses: p.addresses,
      discoveredAt: p.discoveredAt,
      disconnectedAt: p.disconnectedAt,
      messagesTo: pm?.sent ?? 0,
      messagesFrom: pm?.received ?? 0,
      role: p.agentName ? AGENT_ROLES[p.agentName] : undefined,
    });
  }

  // Add untracked libp2p connections (peers connected but not yet in store)
  if (meshNode) {
    try {
      const connPeers: string[] = meshNode.libp2p.getPeers?.()?.map((p: any) => p.toString()) ?? [];
      for (const cp of connPeers) {
        if (cp === store.peerId) continue;
        if (!store.peers.has(cp)) {
          peerList.push({
            peerId: cp,
            agentName: undefined,
            status: "connected" as const,
            addresses: [],
            discoveredAt: Date.now(),
            messagesTo: 0,
            messagesFrom: 0,
          });
        }
      }
    } catch { /* ignore */ }
  }

  const connectedPeers = store.peers.size > 0
    ? [...store.peers.values()].filter(p => p.status === "connected" && p.id !== store.peerId).length
    : 0;

  // Compute messages per second over last 5 seconds
  const now = Date.now();
  mcStats.recentMsgTimestamps = mcStats.recentMsgTimestamps.filter(t => now - t < 5000);
  const msgPerSec = mcStats.recentMsgTimestamps.length / 5;

  // Comm links
  const commLinks = [...mcStats.commLinks.entries()].map(([key, val]) => {
    const [from, to] = key.split('→');
    const avgRttMs = val.rttSamples > 0
      ? Math.round(val.totalRtt / val.rttSamples * 10) / 10
      : 0;
    return { from, to, count: val.count, lastTimestamp: val.lastTimestamp, avgRttMs };
  });

  return {
    self: {
      peerId: store.peerId,
      agentName: store.agentName,
      addresses: resolveLocalIp(meshNode?.libp2p)
        ? [resolveLocalIp(meshNode!.libp2p)!]
        : [],
    },
    peers: peerList,
    stats: {
      totalPeers: store.peers.size > 0 ? store.peers.size - 1 : 0, // exclude self
      connectedPeers,
      messagesSent: mcStats.messagesSent,
      messagesReceived: mcStats.messagesReceived,
      broadcastsSent: mcStats.broadcastsSent,
      broadcastsReceived: mcStats.broadcastsReceived,
      messagesPerSec: Math.round(msgPerSec * 10) / 10,
      pendingQueueDepth: pendingResolvers.length,
      errors: mcStats.errors,
    },
    commLinks,
  };
}

function buildConfig(pi: ExtensionAPI): MeshConfig {
  const swarmKeyPath =
    (pi.getFlag("mesh-swarm-key") as string) ||
    process.env.PI_SWARM_KEY ||
    undefined;

  return {
    agentName: store.agentName,
    enableMdns: true,
    enableDht: pi.getFlag("mesh-enable-dht") as boolean,
    gossipTopic: (pi.getFlag("mesh-gossip-topic") as string) || "pi-broadcast",
    listenPorts: { tcp: 0, ws: 0 },
    swarmKeyPath,
    chromaHost: (pi.getFlag("mesh-chroma-host") as string) || process.env.CHROMA_HOST || undefined,
    chromaPort: parseOptionalInt(pi.getFlag("mesh-chroma-port") as string) ??
      (process.env.CHROMA_PORT ? Number(process.env.CHROMA_PORT) : undefined),
    chromaToken: (pi.getFlag("mesh-chroma-token") as string) || process.env.CHROMA_TOKEN || undefined,
    chromaDataPath: (pi.getFlag("mesh-chroma-data-path") as string) ||
      process.env.CHROMA_DATA_PATH || undefined,
  };
}

/** Record an incoming message for per-peer & comm-link tracking. */
function recordMsgReceived(fromPeerId: string) {
  const pm = mcStats.peerMessages.get(fromPeerId) ?? { sent: 0, received: 0 };
  pm.received++;
  mcStats.peerMessages.set(fromPeerId, pm);
  mcStats.recentMsgTimestamps.push(Date.now());

  // Comm link tracking (reverse direction)
  const linkKey = `${fromPeerId}→${store.peerId}`;
  const existing = mcStats.commLinks.get(linkKey) ?? { count: 0, lastTimestamp: 0, totalRtt: 0, rttSamples: 0 };
  existing.count++;
  existing.lastTimestamp = Date.now();
  mcStats.commLinks.set(linkKey, existing);
}

/** Record an outgoing message for per-peer & comm-link tracking. */
function recordMsgSent(toPeerId: string) {
  const pm = mcStats.peerMessages.get(toPeerId) ?? { sent: 0, received: 0 };
  pm.sent++;
  mcStats.peerMessages.set(toPeerId, pm);
  mcStats.recentMsgTimestamps.push(Date.now());

  // Comm link tracking
  const linkKey = `${store.peerId}→${toPeerId}`;
  const existing = mcStats.commLinks.get(linkKey) ?? { count: 0, lastTimestamp: 0, totalRtt: 0, rttSamples: 0 };
  existing.count++;
  existing.lastTimestamp = Date.now();
  mcStats.commLinks.set(linkKey, existing);
}

// ── Event Handler ────────────────────────────────────────────────────────────

function handleNodeEvent(pi: ExtensionAPI, ev: MeshNodeEvent) {
  switch (ev.type) {
    case "peer:discovered": {
      const existing = store.peers.get(ev.peer.id);
      if (existing) {
        // Merge addresses (discovery may fire after connect/identify)
        const existingAddrs = new Set(existing.addresses);
        for (const addr of ev.peer.addresses) existingAddrs.add(addr);
        existing.addresses = [...existingAddrs];
        // Don't downgrade from connected to disconnected if we already have an active connection
        existing.discoveredAt = ev.peer.discoveredAt;
      } else {
        store.peers.set(ev.peer.id, ev.peer);
      }
      if (meshProtocols) meshProtocols.handlePeerDiscovered(ev.peer);
      notify(pi, `Peer discovered: ${ev.peer.id} (${ev.peer.addresses.join(", ")})`);
      break;
    }

    case "peer:connected": {
      let p = store.peers.get(ev.peerId);
      if (!p) {
        // Inbound connection before mDNS discovery — create a placeholder
        p = {
          id: ev.peerId,
          addresses: [],
          status: "connected",
          discoveredAt: Date.now(),
        };
        store.peers.set(ev.peerId, p);
      } else {
        p.status = "connected";
      }
      // Push immediate state update to mission control
      if (missionControl) {
        missionControl.updateState(buildMissionControlState());
      }
      notify(pi, `Peer connected: ${ev.peerId}`);
      break;
    }

    case "peer:disconnected": {
      const p = store.peers.get(ev.peerId);
      if (p) { p.status = "disconnected"; p.disconnectedAt = Date.now(); }
      if (missionControl) {
        missionControl.updateState(buildMissionControlState());
      }
      notify(pi, `Peer disconnected: ${ev.peerId}`);
      break;
    }

    case "peer:identified": {
      let p = store.peers.get(ev.peerId);
      if (!p) {
        // Identify before discovery — create a placeholder
        p = {
          id: ev.peerId,
          addresses: [],
          status: "connected",
          discoveredAt: Date.now(),
        };
        store.peers.set(ev.peerId, p);
      }
      if (ev.agentName) {
        p.agentName = ev.agentName;
        notify(pi, `Peer identified: ${ev.peerId.slice(0, 12)}… as "${ev.agentName}"`);
      }
      // Store extension version for async LLM compat detection
      if (ev.extensionVersion) {
        p.extensionVersion = ev.extensionVersion;
      }
      // Detect stale builds: if the peer's extension version differs from ours, warn
      if (ev.extensionVersion && ev.extensionVersion !== EXTENSION_VERSION) {
        notify(
          pi,
          `⚠️ Version mismatch with ${ev.agentName || ev.peerId.slice(0, 12) + "…"}: they run v${ev.extensionVersion}, we run v${EXTENSION_VERSION}`,
          "warning",
        );
      }
      break;
    }

    case "message":
      // Incoming direct message — could forward to LLM via pi.sendMessage
      // For now we log; the LLM accesses via tools.
      notify(pi, `Message from ${ev.request.fromAgent}: ${ev.request.message.slice(0, 120)}`);
      break;

    case "broadcast":
      recordBroadcast(store, ev.message);
      notify(pi, `Broadcast from ${ev.message.fromAgent}: ${ev.message.message.slice(0, 120)}`);
      break;

  }
}

// ── Extension Entry Point ────────────────────────────────────────────────────

export default async function (pi: ExtensionAPI) {
  // 0. Register CLI flags
  const hostname = os.hostname();
  pi.registerFlag("agent-name", {
    description: `Agent name for the P2P mesh (default: pi-${hostname}, or PI_MESH_NAME env var)`,
    type: "string",
    default: "",
  });
  pi.registerFlag("mesh-enable-dht", {
    description: "Enable Kademlia DHT for wide-area peer discovery",
    type: "boolean",
    default: false,
  });
  pi.registerFlag("mesh-gossip-topic", {
    description: "GossipSub topic for broadcast messages",
    type: "string",
    default: "pi-broadcast",
  });
  pi.registerFlag("mesh-swarm-key", {
    description:
      "Path to a swarm.key file for private P2P network (also PI_SWARM_KEY env var). " +
      "All peers must share the same key.",
    type: "string",
    default: "",
  });

  // Memory / ChromaDB flags
  pi.registerFlag("mesh-chroma-host", {
    description: "ChromaDB server hostname (default: localhost, or CHROMA_HOST env var)",
    type: "string",
    default: "",
  });
  pi.registerFlag("mesh-chroma-port", {
    description: "ChromaDB server port (default: 8000, or CHROMA_PORT env var)",
    type: "string",
    default: "",
  });
  pi.registerFlag("mesh-chroma-token", {
    description:
      "Auth token for ChromaDB (x-chroma-token header). " +
      "Also set via CHROMA_TOKEN env var.",
    type: "string",
    default: "",
  });
  pi.registerFlag("mesh-chroma-data-path", {
    description:
      "ChromaDB data directory for persistence (default: ~/.local/share/chroma). " +
      "Also CHROMA_DATA_PATH env var.",
    type: "string",
    default: "",
  });
  pi.registerFlag("mesh-memory-preset", {
    description:
      "Memory limit preset: small (32K), medium (128K), large 1M (default). " +
      "Sets all read-side limits at once. Also PI_MEMORY_PRESET env var.",
    type: "string",
    default: "large",
  });
  pi.registerFlag("mesh-memory-max-entries", {
    description: "Override hard max entries returned by memory_recall",
    type: "string",
    default: "",
  });
  pi.registerFlag("mesh-memory-truncate", {
    description: "Override value truncation chars for memory entries",
    type: "string",
    default: "",
  });
  pi.registerFlag("mesh-memory-budget", {
    description: "Override auto-retrieve context budget in chars",
    type: "string",
    default: "",
  });
  pi.registerFlag("mesh-memory-exchange-truncate", {
    description: "Override exchange truncation in chars",
    type: "string",
    default: "",
  });
  pi.registerFlag("mesh-memory-distance", {
    description: "Override distance threshold for search filtering",
    type: "string",
    default: "",
  });
  pi.registerFlag("mesh-mc-port", {
    description:
      "Port for the mission-control 3D network dashboard (default: 9191). " +
      "Starts automatically on session_start.",
    type: "string",
    default: "",
  });

  // 1. Session lifecycle: start node
  pi.on("session_start", async (_event, ctx) => {
    sessionActive = true;
    // Drain any stale resolvers from a previous session crash
    pendingResolvers.length = 0;
    pendingMeshTurns = 0;
    // Resolve agent name now (CLI flags are parsed at this point):
    //   1. --agent-name CLI flag (explicit)
    //   2. PI_MESH_NAME or PI_COMM_NAME env var (backward compat with pi-comm)
    //   3. Default: pi-<hostname>
    const flagName = pi.getFlag("agent-name") as string;
    const envName = process.env.PI_MESH_NAME || process.env.PI_COMM_NAME;
    store.agentName = flagName || envName || `pi-${hostname}`;

    const config = buildConfig(pi);

    // Resolve memory config from CLI flags and env vars
    const preset = (pi.getFlag("mesh-memory-preset") as string) ||
      process.env.PI_MEMORY_PRESET || "large";
    const memoryConfig = resolveMemoryConfig({
      preset,
      maxEntries: parseOptionalInt(pi.getFlag("mesh-memory-max-entries")) ??
        (process.env.PI_MEMORY_MAX_ENTRIES ? Number(process.env.PI_MEMORY_MAX_ENTRIES) : undefined),
      truncate: parseOptionalInt(pi.getFlag("mesh-memory-truncate")) ??
        (process.env.PI_MEMORY_TRUNCATE ? Number(process.env.PI_MEMORY_TRUNCATE) : undefined),
      budget: parseOptionalInt(pi.getFlag("mesh-memory-budget")) ??
        (process.env.PI_MEMORY_BUDGET ? Number(process.env.PI_MEMORY_BUDGET) : undefined),
      exchangeTruncate: parseOptionalInt(pi.getFlag("mesh-memory-exchange-truncate")) ??
        (process.env.PI_MEMORY_EXCHANGE_TRUNCATE ? Number(process.env.PI_MEMORY_EXCHANGE_TRUNCATE) : undefined),
      distance: parseOptionalFloat(pi.getFlag("mesh-memory-distance")) ??
        (process.env.PI_MEMORY_DISTANCE ? Number(process.env.PI_MEMORY_DISTANCE) : undefined),
    });

    try {
      meshNode = await MeshNode.create(config, EXTENSION_VERSION);
      meshProtocols = new MeshProtocols(meshNode.libp2p, config, EXTENSION_VERSION);

      // Wire protocols into tools so mesh_send / mesh_broadcast work
      setMeshProtocols(meshProtocols);

      // Incoming direct messages — forward via pi's event bus
      meshProtocols.onMessage = (_peerId, request) => {
        // Detect stale peers: if the request includes a version and it doesn't match
        // ours, log a warning (identify protocol already handles this, but this is
        // an extra signal for the operator).
        if ((request as any).extensionVersion && (request as any).extensionVersion !== EXTENSION_VERSION) {
          console.warn(
            `[pi-libp2p-mesh] Version mismatch: peer ${request.fromAgent} runs v${(request as any).extensionVersion}, we run v${EXTENSION_VERSION}`,
          );
        }
        handleNodeEvent(pi, { type: "message", fromPeerId: request.fromPeerId, request });
      };

      // ── LLM Request Queue (FIFO) ────────────────────────────────────────
      // Uses a module-level FIFO resolver queue paired with the `agent_end`
      // event for reliable 1:1 request-response mapping.

      const MAX_QUEUE_SIZE = 50;

      /** Extract assistant text from an agent_end event's messages array. */
      function extractResponseFromMessages(messages: any[]): string {
        if (!messages || !Array.isArray(messages)) return "[no response]";
        // Walk backward to find the last assistant message
        for (let i = messages.length - 1; i >= 0; i--) {
          const msg = messages[i];
          if (msg?.role === "assistant") {
            if (typeof msg.content === "string") return msg.content || "[empty response]";
            if (Array.isArray(msg.content)) {
              const text = msg.content
                .filter((c: any) => c.type === "text")
                .map((c: any) => c.text)
                .join("\n");
              return text || "[empty response]";
            }
            return "[non-text response]";
          }
        }
        return "[no assistant response]";
      }

      /** Deliver an async response (or heartbeat) to a requesting peer. */
      async function deliverAsyncResponse(
        targetPeerId: string,
        responseToRequestId: string,
        message: string,
      ): Promise<void> {
        if (!meshProtocols) return;
        await meshProtocols.sendMessage(targetPeerId, {
          protocol: "/pi-agent/0.1.0",
          requestId: crypto.randomUUID?.() ?? `${Date.now()}-${Math.random()}`,
          fromAgent: store.agentName,
          message,
          responseToRequestId,
        });
      }

      // Use agent_end instead of turn_end for perfect request-response pairing.
      // Each sendUserMessage() call triggers exactly one agent cycle, and each
      // agent cycle produces exactly one agent_end event.
      pi.on("agent_end", (_event) => {
        if (!sessionActive) return; // Guard against duplicate handlers after session reload
        // Only consume a resolver if this agent_end was triggered by a mesh
        // request (steer-delivered message). User chat agent_end events must
        // not steal resolvers from the queue.
        if (pendingMeshTurns <= 0) return;
        pendingMeshTurns--;

        const pending = pendingResolvers.shift();
        if (!pending) return; // No mesh request waiting — must be user chat

        clearTimeout(pending.timer);

        // Always deliver the real response, even if heartbeats were sent.
        // Late is better than never — the sender gets the full answer.
        const responseText = extractResponseFromMessages((_event as any).messages);
        if (pending.resolved) {
          // Heartbeats already resolved with timeout — deliver late response
          // as a follow-up mesh_send with the real answer.
          deliverAsyncResponse(pending.peerId, pending.requestId, responseText)
            .catch((err) => console.warn("[pi-libp2p-mesh] late response delivery failed:", (err as Error).message));
        } else {
          pending.resolve(responseText);
        }
      });

      // Incoming LLM-forward requests (autoReply !== true) — enqueue into FIFO
      meshProtocols.onRequest = async (peerId, request) => {
        // ── Async response delivery ───────────────────────────────────
        // When responseToRequestId is set, this is an async response to a
        // previous request we sent. Store silently in memory — do NOT
        // trigger an LLM turn (which would create a bottleneck when multiple
        // responses arrive simultaneously). The LLM retrieves responses via
        // memory context injection on its next interaction.
        if (request.responseToRequestId) {
          const peer = store.peers.get(peerId);

          // Store response in memory for future retrieval (silent, no LLM turn)
          if (agentMemory) {
            agentMemory.store({
              peerId,
              key: "async_response",
              value: `[Response from ${request.fromAgent} (re: ${request.responseToRequestId})] ${request.message}`,
              metadata: { type: "async_response", requestId: request.responseToRequestId, fromAgent: request.fromAgent },
            }).catch((err) => console.warn("[pi-libp2p-mesh] async response store failed:", (err as Error).message));
          }

          // Emit to mission control
          if (missionControl) {
            const flow: MessageFlow = {
              from: peerId,
              fromName: peer?.agentName ?? peerId.slice(0, 12) + "…",
              to: store.peerId,
              toName: store.agentName,
              direction: "received",
              sizeBytes: Buffer.byteLength(request.message, "utf-8"),
              timestamp: Date.now(),
            };
            missionControl.emitMessage(flow);
          }
          mcStats.messagesReceived++;
          recordMsgReceived(peerId);

          // Auto-ACK — don't queue an LLM turn for this delivery
          return `[received] Async response stored in memory`;
        }

        // If global auto-reply-all is on, echo without LLM
        if (store.autoReplyAll) {
          return `[auto-reply-all] Received: "${request.message}"`;
        }

        mcStats.messagesReceived++;
        recordMsgReceived(peerId);

        // Emit message flow to mission control
        if (missionControl) {
          const peer = store.peers.get(peerId);
          const flow: MessageFlow = {
            from: peerId,
            fromName: peer?.agentName ?? peerId.slice(0, 12) + "…",
            to: store.peerId,
            toName: store.agentName,
            direction: "received",
            sizeBytes: Buffer.byteLength(request.message, "utf-8"),
            timestamp: Date.now(),
          };
          missionControl.emitMessage(flow);
        }

        // Backpressure: reject the request immediately if queue is full
        if (pendingResolvers.length >= MAX_QUEUE_SIZE) {
          return `[queue-full] Agent request queue is full (max ${MAX_QUEUE_SIZE}). Please retry later.`;
        }

        // Auto-retrieve memory context about the requesting peer
        let memoryContext: string | undefined;
        if (agentMemory) {
          try {
            memoryContext = await buildMemoryContext(request, peerId);
          } catch (err) {
            console.debug("[pi-libp2p-mesh] auto-retrieve failed:", (err as Error).message);
          }
        }

        // Build the user message with optional memory context
        let userMessage = `[Mesh message from ${request.fromAgent}]\n\n${request.message}`;
        if (memoryContext) {
          userMessage = memoryContext + "\n\n" + userMessage;
        }

        return new Promise<string>((resolve) => {
          const HEARTBEAT_INTERVAL_MS = 30_000;
          const MAX_HEARTBEATS = 4; // 4 × 30s = 120s total
          let heartbeatCount = 0;

          // Push resolver BEFORE creating the heartbeat timer so the
          // pending object is available for sendHeartbeat to update.
          const pending: PendingResolver = {
            resolve,
            timer: null as any, // set below
            resolved: false,
            peerId,
            requestId: request.requestId,
            messagePreview: request.message.slice(0, 100),
          };
          pendingResolvers.push(pending);

          const sendHeartbeat = () => {
            heartbeatCount++;
            if (heartbeatCount >= MAX_HEARTBEATS) {
              // Max heartbeats reached — resolve with timeout
              pending.resolved = true;
              resolve(
                `[timeout] Agent did not respond within ${MAX_HEARTBEATS * HEARTBEAT_INTERVAL_MS / 1000}s to: "${request.message.slice(0, 100)}${request.message.length > 100 ? "…" : ""}"`,
              );
            } else {
              // Send heartbeat to requester so they know we're still working
              deliverAsyncResponse(peerId, request.requestId,
                `[working… ${heartbeatCount * HEARTBEAT_INTERVAL_MS / 1000}s] Still processing: "${request.message.slice(0, 60)}${request.message.length > 60 ? "…" : ""}"`
              ).catch(() => { /* best-effort */ });
              // Re-arm timer for next heartbeat
              pending.timer = setTimeout(sendHeartbeat, HEARTBEAT_INTERVAL_MS);
            }
          };

          pending.timer = setTimeout(sendHeartbeat, HEARTBEAT_INTERVAL_MS);

          // Fire off the LLM request — agent_end will resolve our promise
          pendingMeshTurns++;
          pi.sendUserMessage(userMessage, { deliverAs: "steer" });
        }).then((responseText) => {
          mcStats.messagesSent++;
          recordMsgSent(peerId);
          if (missionControl) {
            const peer = store.peers.get(peerId);
            const flow: MessageFlow = {
              from: store.peerId,
              fromName: store.agentName,
              to: peerId,
              toName: peer?.agentName ?? peerId.slice(0, 12) + "…",
              direction: "sent",
              sizeBytes: Buffer.byteLength(responseText, "utf-8"),
              timestamp: Date.now(),
            };
            missionControl.emitMessage(flow);
          }

          // ── Send async response back to requester ─────────────────
          // Instead of holding the stream open for 10-60s waiting for the
          // LLM, we already ACK'd the stream immediately in protocols.ts.
          // Now we deliver the real response via a follow-up mesh_send.

          // Track RTT: elapsed from sender's original request timestamp to now.
          const rtt = Date.now() - request.timestamp;
          const rttLinkKey = `${store.peerId}→${peerId}`;
          let rttLink = mcStats.commLinks.get(rttLinkKey);
          if (!rttLink) {
            rttLink = { count: 0, lastTimestamp: 0, totalRtt: 0, rttSamples: 0 };
            mcStats.commLinks.set(rttLinkKey, rttLink);
          }
          rttLink.totalRtt += rtt;
          rttLink.rttSamples++;

          if (meshProtocols) {
            meshProtocols.sendMessage(peerId, {
              protocol: "/pi-agent/0.1.0",
              requestId: crypto.randomUUID?.() ?? `${Date.now()}-${Math.random()}`,
              fromAgent: store.agentName,
              message: responseText,
              // responseToRequestId tells the receiver this is an async
              // response — it forwards to LLM and auto-ACKs (no LLM loop).
              responseToRequestId: request.requestId,
            }).catch((err) => {
              console.warn("[pi-libp2p-mesh] Failed to deliver async response:", (err as Error).message);
            });
          }

          // Auto-save exchange to memory (fire-and-forget, non-blocking)
          if (agentMemory) {
            agentMemory.store({
              peerId,
              key: "exchange",
              value: `[Request from ${request.fromAgent}] ${request.message}\n[Response] ${responseText}`,
              metadata: { type: "conversation_turn", requestId: request.requestId },
            }).catch((err) => {
              console.warn("[pi-libp2p-mesh] exchange auto-save failed:", (err as Error).message);
            });
          }
          return responseText;
        });
      };

      // Incoming broadcasts — record in store, notify, and optionally forward to LLM
      meshProtocols.onBroadcast = (msg) => {
        // Skip our own broadcasts — GossipSub delivers to all subscribers
        // including the publisher, creating a feedback loop if we re-process
        if (meshNode && msg.fromPeerId === meshNode.peerId) return;

        handleNodeEvent(pi, { type: "broadcast", message: msg });

        // If auto-reply-all is off, forward the broadcast to the LLM so the agent
        // can process it (e.g. for coordination, awareness of announcements, etc.).
        // Broadcasts are fire-and-forget (no response expected).
        if (!store.autoReplyAll) {
          pi.sendUserMessage(
            `[Mesh broadcast from ${msg.fromAgent} (${msg.type ?? "announce"})]\n\n${msg.message}`,
            { deliverAs: "steer" },
          );
        }

        mcStats.broadcastsReceived++;
        recordMsgReceived(msg.fromPeerId);

        // Emit broadcast flow to mission control
        if (missionControl) {
          const flow: BroadcastFlow = {
            from: msg.fromPeerId,
            fromName: msg.fromAgent,
            message: msg.message,
            type: msg.type,
            timestamp: Date.now(),
          };
          missionControl.emitBroadcast(flow);
        }

        // Auto-save broadcast to memory
        if (agentMemory) {
          agentMemory.store({
            peerId: msg.fromPeerId,
            key: "broadcast",
            value: `[${msg.type ?? "announce"}] ${msg.message}`,
            metadata: { type: "broadcast" },
          }).catch((err) => {
            console.warn("[pi-libp2p-mesh] broadcast auto-save failed:", (err as Error).message);
          });
        }
      };

      // ── Mesh-aware system prompt guidance ────────────────────────────
      // Inject best practices for delegating work across the P2P mesh.
      pi.on("before_agent_start", (event: any) => {
        if (!sessionActive) return; // Guard against duplicate handlers after session reload
        event.systemPrompt += `\n\n## P2P Mesh Guidelines\n` +
          `- When delegating work to other agents via mesh_send, decompose large tasks into small subtasks that each complete within the heartbeat timeout (~30s).\n` +
          `- A single mesh_send that asks an agent to \"analyze this entire file for 5 categories of bugs\" will likely time out. Instead send 3-4 focused queries.\n` +
          `- The heartbeat/late-delivery system is a safety net — not the primary strategy. Small, focused subtasks are always better.\n` +
          `- Use autoReply for simple pings/checks. Use full LLM queries only when you need reasoning.\n`;
      });

      /**
       * Build memory context for an incoming request from a peer.
       * Combines semantic search + most recent exchange, capped by config budget.
       */
      async function buildMemoryContext(
        request: import("./types.js").AgentRequest,
        peerId: string,
      ): Promise<string | undefined> {
        if (!agentMemory) return undefined;

        const peer = store.peers.get(peerId);
        const name = peer?.agentName ?? peerId.slice(0, 12) + "…";

        // Semantic search for relevant memories (get full values, truncate once below)
        const searchResults = await agentMemory.search(request.message, {
          peerId,
          nResults: 3,
          fullText: true, // avoid double-truncation: we slice to budget below
        });

        // Most recent exchange (get full value, truncate once below)
        const recentExchanges = await agentMemory.get(peerId, "exchange", {
          limit: 1,
          fullText: true,
        });

        // Build context lines, respecting budget
        const lines: string[] = [];
        let charCount = 0;
        const budget = agentMemory.config.contextBudgetChars;

        const addLine = (line: string) => {
          if (charCount + line.length > budget) return;
          lines.push(line);
          charCount += line.length;
        };

        addLine(`[Memory about ${name}:]`);

        // Add semantic search results
        for (const r of searchResults) {
          addLine(`  ${r.key}: ${r.value.slice(0, agentMemory.config.exchangeTruncationChars)}`);
        }

        // Add most recent exchange (truncated)
        if (recentExchanges.length > 0) {
          const ex = recentExchanges[0];
          addLine(
            `  Last exchange: ${ex.value.slice(0, agentMemory.config.exchangeTruncationChars)}`,
          );
        }

        // Count
        try {
          const total = await agentMemory.count(peerId);
          addLine(`  (${total} total interactions)`);
        } catch {
          // ignore
        }

        return lines.length > 1 ? lines.join("\n") : undefined;
      }

      // Forward node events into our handler
      meshNode.onEvent((ev) => handleNodeEvent(pi, ev));

      await meshNode.start();
      store.peerId = meshNode.peerId;

      notify(pi, `Mesh node started as "${config.agentName}" (${meshNode.peerId})`);

      // ── ChromaDB Host Election ───────────────────────────────────────
      // First node to start becomes the host. Others connect to it.
      // Discovery is via GossipSub on the "pi-memory-host" topic.
      //
      // Local ChromaDB start runs in parallel with listening for remote
      // host announcements — whichever resolves first wins. This eliminates
      // the fixed 2.5 s delay when a solo agent starts up.
      const defaultHost = config.chromaHost ?? "localhost";
      const defaultPort = config.chromaPort ?? 8000;
      let resolvedHost = defaultHost;
      let resolvedPort = defaultPort;
      let isMemoryHost = false;
      let hostResolved = false; // set when we definitively pick local or remote

      // Resolve the local IP we'll advertise if we become the host
      const localIp = resolveLocalIp(meshNode.libp2p);

      chromaLifecycle = new ChromaDBLifecycle({
        host: defaultHost,
        port: defaultPort,
        token: config.chromaToken,
        dataPath: config.chromaDataPath,
      });

      // Subscribe to host announcements from other peers.
      // If a remote host announces before our local start completes, we
      // abort our local start and connect to the remote host instead.
      meshProtocols.subscribeRawTopic<import("./types.js").MemoryHostAnnouncement>(
        "pi-memory-host",
        (announcement) => {
          if (hostResolved) return; // already decided — ignore late announcements
          hostResolved = true;
          resolvedHost = announcement.host;
          resolvedPort = announcement.port;
          isMemoryHost = false;
          chromaLifecycle!.abort(); // cancel any in-progress local start
          notify(pi, `Discovered ChromaDB host: ${announcement.fromAgent} at ${announcement.host}:${announcement.port}`);
        },
      );

      // Helper: announce ourselves as the ChromaDB host
      const announceAsHost = async () => {
        const announcement: import("./types.js").MemoryHostAnnouncement = {
          type: "memory:host",
          host: resolvedHost,
          port: resolvedPort,
          fromAgent: store.agentName,
          fromPeerId: meshNode!.peerId,
          timestamp: Date.now(),
        };
        await meshProtocols!.publishRawTopic("pi-memory-host", announcement);
      };

      // Start local ChromaDB readiness as a background promise.
      // This runs in parallel with listening for remote host announcements
      // so that a solo agent doesn't pay the 2.5 s wait penalty.
      const localReady = (async (): Promise<boolean> => {
        const running = await chromaLifecycle!.isRunning();
        if (running) {
          if (!hostResolved) {
            hostResolved = true;
            resolvedHost = localIp ?? defaultHost;
            resolvedPort = defaultPort;
            isMemoryHost = true;
            notify(pi, `ChromaDB already running locally — announcing as host at ${resolvedHost}:${resolvedPort}`);
            await announceAsHost();
          }
          return true;
        }
        // Not running — spawn it. ensureRunning() checks the abort flag
        // so we can cancel mid-start if a remote host appears.
        const started = await chromaLifecycle!.ensureRunning();
        if (started && !hostResolved) {
          hostResolved = true;
          resolvedHost = localIp ?? defaultHost;
          resolvedPort = defaultPort;
          isMemoryHost = true;
          await announceAsHost();
          return true;
        }
        return false;
      })();

      // Race local startup against a 2.5 s window for remote hosts.
      // If local finishes first (the common solo-agent case), we skip the
      // wait entirely. If a remote host announces, we abort local and
      // connect to remote.
      await Promise.race([
        localReady,
        new Promise<void>((r) => setTimeout(r, 2500)),
      ]);

      // If neither resolved yet (local still starting, no remote heard),
      // wait for local to finish.
      if (!hostResolved) {
        const ok = await localReady;
        if (!ok) {
          console.warn("[pi-libp2p-mesh] Failed to start ChromaDB locally — memory tools disabled");
        }
      } else if (!isMemoryHost) {
        // Remote host won — stop our local ChromaDB if we started it
        chromaLifecycle.stop();
        notify(pi, `Connecting to mesh ChromaDB host at ${resolvedHost}:${resolvedPort}`);
      }

      // ── Initialize AgentMemory ───────────────────────────────────────
      try {
        agentMemory = await AgentMemory.create({
          host: resolvedHost,
          port: resolvedPort,
          token: config.chromaToken,
          agentName: store.agentName,
          config: memoryConfig,
        });
        setAgentMemory(agentMemory);
        const role = isMemoryHost ? "(host)" : "";
        notify(pi, `Memory connected ${role} — collection "${agentMemory.collectionName}" at ${resolvedHost}:${resolvedPort}`);
        ctx.ui.notify(`🧠 ChromaDB memory active — collection "${agentMemory.collectionName}" ${role}`, "info");
      } catch (err: any) {
        const reason = err.message || String(err);
        console.warn(`[pi-libp2p-mesh] Memory initialization failed — memory tools disabled for this session: ${reason}`);
        ctx.ui.notify(`⚠️ ChromaDB memory unavailable: ${reason}. Memory tools disabled.`, "warning");
        agentMemory = null;
        setAgentMemory(null);
      }

      // Background stale-peer pruning — runs every 30s to keep the peer
      // table clean without explicit LLM-triggered prune commands.
      pruneInterval = setInterval(() => {
        const removed = pruneStalePeers(store);
        if (removed > 0) {
          notify(pi, `Background prune: removed ${removed} stale peer(s)`);
        }
      }, 30_000);

      // Periodic cleanup of stale RTT entries (prevents unbounded growth)
      rttCleanupInterval = setInterval(() => {
        cleanupStaleRtt();
      }, RTT_CLEANUP_INTERVAL_MS);

      // ── Mission Control Server ─────────────────────────────────────
      // Start the HTTP+WS server for the 3D network visualization.
      // State is pushed every 2 s so the dashboard stays in sync.
      try {
        const mcPort = parseOptionalInt(pi.getFlag("mesh-mc-port")) ?? 9191;
        missionControl = new MissionControlServer({ port: mcPort });
        await missionControl.start();

        // Initial state push
        missionControl.updateState(buildMissionControlState());

        // Periodic state refresh
        missionControlInterval = setInterval(() => {
          if (missionControl) {
            missionControl.updateState(buildMissionControlState());
          }
        }, 2_000);

        ctx.ui.notify(
          `🛰 Mission control: http://localhost:${mcPort}`,
          "info",
        );
      } catch (err: any) {
        console.warn(
          `[pi-libp2p-mesh] Mission control server failed to start: ${err.message}`,
        );
      }

      // Periodic ChromaDB host re-announcement — ensures late joiners
      // and nodes that missed the initial announcement can discover us.
      if (isMemoryHost) {
        memoryHostAnnounceInterval = setInterval(() => {
          const announcement: import("./types.js").MemoryHostAnnouncement = {
            type: "memory:host",
            host: resolvedHost,
            port: resolvedPort,
            fromAgent: store.agentName,
            fromPeerId: meshNode!.peerId,
            timestamp: Date.now(),
          };
          meshProtocols!.publishRawTopic("pi-memory-host", announcement).catch((err) =>
            console.warn("[pi-libp2p-mesh] Failed to re-announce ChromaDB host:", err),
          );
        }, 10_000);
      }

      ctx.ui.notify(
        `libp2p mesh online — ${meshNode.peerId}`,
        "info",
      );
    } catch (err: any) {
      notify(pi, `Failed to start mesh node: ${err.message}`, "error");
    }
  });

  // 2. Session lifecycle: stop node
  pi.on("session_shutdown", async () => {
    sessionActive = false;
    pendingMeshTurns = 0;
    // Drain and stale any pending request queue entries
    for (const pending of pendingResolvers) {
      clearTimeout(pending.timer);
      if (!pending.resolved) {
        pending.resolve("[shutdown] Session ended while request was queued");
      }
    }
    pendingResolvers.length = 0;

    // Stop background pruning
    if (pruneInterval) {
      clearInterval(pruneInterval);
      pruneInterval = null;
    }
    // Stop RTT cleanup
    if (rttCleanupInterval) {
      clearInterval(rttCleanupInterval);
      rttCleanupInterval = null;
    }
    // Stop mission control
    if (missionControlInterval) {
      clearInterval(missionControlInterval);
      missionControlInterval = null;
    }
    if (missionControl) {
      await missionControl.stop();
      missionControl = null;
    }
    // Stop memory host re-announcement
    if (memoryHostAnnounceInterval) {
      clearInterval(memoryHostAnnounceInterval);
      memoryHostAnnounceInterval = null;
    }
    if (meshProtocols) {
      await meshProtocols.stop();
    }
    if (meshNode) {
      await meshNode.stop();
    }
    meshNode = null;
    meshProtocols = null;
    setMeshProtocols(null);
    if (agentMemory) {
      await agentMemory.stop();
      agentMemory = null;
      setAgentMemory(null);
    }
    if (chromaLifecycle) {
      chromaLifecycle.stop();
      chromaLifecycle = null;
    }
    // Clear all peer and broadcast state so the next session starts fresh
    // (the module-level store survives across session restarts because
    //  Node.js caches the extension module)
    store.peers.clear();
    store.broadcastHistory = [];
    notify(pi, "Mesh node stopped — state cleared");
  });

  // 3. Register mesh tools
  registerMeshTools(pi, store);

  // 3b. Register memory tools
  registerMemoryTools(pi, store);

  // 4. Register commands for manual control
  pi.registerCommand("mesh-auto-reply", {
    description: "Toggle auto-reply mode (when on, all incoming mesh messages echo without LLM)",
    handler: async (args, ctx) => {
      const arg = args.trim().toLowerCase();
      if (arg === "on" || arg === "true" || arg === "1") {
        store.autoReplyAll = true;
        ctx.ui.notify("Auto-reply: ON — incoming mesh messages will echo without LLM", "info");
      } else if (arg === "off" || arg === "false" || arg === "0") {
        store.autoReplyAll = false;
        ctx.ui.notify("Auto-reply: off — incoming mesh messages will be forwarded to LLM", "info");
      } else if (arg === "") {
        // Toggle
        store.autoReplyAll = !store.autoReplyAll;
        ctx.ui.notify(
          `Auto-reply: ${store.autoReplyAll ? "ON" : "off"}`,
          "info",
        );
      } else {
        ctx.ui.notify(
          `Usage: /mesh-auto-reply [on|off] — current: ${store.autoReplyAll ? "ON" : "off"}`,
          "warning",
        );
      }
    },
  });

  pi.registerCommand("mesh-list-peers", {
    description: "List all peers on the P2P mesh network",
    handler: async (_args, ctx) => {
      if (!meshNode) {
        ctx.ui.notify("Mesh node not running", "warning");
        return;
      }

      const { peers, connected, total } = listPeers(store);

      if (total === 0) {
        ctx.ui.notify("No peers known. Peer discovery runs automatically via mDNS in the background.", "info");
        return;
      }

      // Sort: self first, then connected, then disconnected
      const sorted = [...peers].sort((a, b) => {
        if (a.id === store.peerId) return -1;
        if (b.id === store.peerId) return 1;
        if (a.status === "connected" && b.status !== "connected") return -1;
        if (a.status !== "connected" && b.status === "connected") return 1;
        return (a.agentName ?? "").localeCompare(b.agentName ?? "");
      });

      const lines = sorted.map((p) => {
        const icon = p.status === "connected" ? "🟢" : "🔴";
        const name = p.agentName ?? "unknown";
        const age = Math.round((Date.now() - p.discoveredAt) / 1000);
        const idDisplay = p.id;
        const selfMarker = p.id === store.peerId ? " (SELF)" : "";
        return `${icon} ${name} — ${idDisplay} (${p.status}, ${age}s ago)${selfMarker}`;
      });

      ctx.ui.notify(
        `${connected}/${total} peers:\n${lines.join("\n")}`,
        "info",
      );
    },
  });

  pi.registerCommand("mesh-discover", {
    description: "Refresh the known peer list — prunes stale entries and shows currently known peers",
    handler: async (_args, ctx) => {
      if (!meshNode) {
        ctx.ui.notify("Mesh node not running", "warning");
        return;
      }

      ctx.ui.notify("Refreshing peer list…", "info");

      // Prune stale peers first so the view is fresh
      const pruned = pruneAllDisconnected(store);

      const { peers, connected, total } = listPeers(store);

      if (total === 0) {
        ctx.ui.notify(
          "No peers known. Peer discovery via mDNS runs in the background — ensure other pi agents with pi-libp2p-mesh are running on the same network.",
          "warning",
        );
        return;
      }

      // Sort: self first, then connected, then disconnected
      const sorted = [...peers].sort((a, b) => {
        if (a.id === store.peerId) return -1;
        if (b.id === store.peerId) return 1;
        if (a.status === "connected" && b.status !== "connected") return -1;
        if (a.status !== "connected" && b.status === "connected") return 1;
        return (a.agentName ?? "").localeCompare(b.agentName ?? "");
      });

      const lines = sorted.map((p) => {
        const icon = p.status === "connected" ? "🟢" : "🔴";
        const name = p.agentName ?? "unknown";
        const age = Math.round((Date.now() - p.discoveredAt) / 1000);
        const selfMarker = p.id === store.peerId ? " (SELF)" : "";
        return `${icon} ${name} — ${p.id} (${p.status}, ${age}s ago)${selfMarker}`;
      });

      ctx.ui.notify(
        `${connected}/${total} peers (${pruned} stale pruned):\n${lines.join("\n")}`,
        "info",
      );
    },
  });

  pi.registerCommand("mesh-prune", {
    description: "Remove all disconnected/stale peers from the peer list",
    handler: async (_args, ctx) => {
      if (!meshNode) {
        ctx.ui.notify("Mesh node not running", "warning");
        return;
      }

      const before = store.peers.size;
      const removed = pruneAllDisconnected(store);
      const after = store.peers.size;
      // After pruning all disconnected, remaining peers are connected
      const connected = after;

      ctx.ui.notify(
        removed === 0
          ? `No stale peers to prune. All ${before} peer(s) connected.`
          : `🧹 Pruned ${removed} stale peer(s). ${before} → ${after} (${connected} connected)`,
        "info",
      );
    },
  });

  pi.registerCommand("mesh-mission-control", {
    description:
      "Open the 3D mission-control network dashboard (starts server if not running)",
    handler: async (_args, ctx) => {
      if (!meshNode) {
        ctx.ui.notify("Mesh node not running", "warning");
        return;
      }

      if (!missionControl) {
        // Start it on demand
        try {
          const mcPort = parseOptionalInt(pi.getFlag("mesh-mc-port")) ?? 9191;
          missionControl = new MissionControlServer({ port: mcPort });
          await missionControl.start();
          missionControl.updateState(buildMissionControlState());

          missionControlInterval = setInterval(() => {
            if (missionControl) {
              missionControl.updateState(buildMissionControlState());
            }
          }, 2_000);

          ctx.ui.notify(
            `🛰 Mission control started: http://localhost:${mcPort}`,
            "info",
          );
        } catch (err: any) {
          ctx.ui.notify(
            `Failed to start mission control: ${err.message}`,
            "error",
          );
          return;
        }
      } else {
        const port = missionControl.port;
        ctx.ui.notify(
          `🛰 Mission control running at http://localhost:${port}`,
          "info",
        );
      }

      // Try to open the browser
      try {
        const { exec } = await import("node:child_process");
        const url = `http://localhost:${missionControl!.port}`;
        const platform = process.platform;
        if (platform === "darwin") {
          exec(`open "${url}"`);
        } else if (platform === "win32") {
          exec(`start "" "${url}"`);
        } else {
          exec(`xdg-open "${url}"`);
        }
      } catch {
        // Non-fatal — user can open manually
      }
    },
  });
}

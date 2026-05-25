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
import { registerMeshTools, registerMemoryTools, setMeshProtocols, setAgentMemory, listPeers, pruneAllDisconnected, pruneStalePeers, recordBroadcast, type MeshStore } from "./tools.js";
import { AgentMemory, resolveMemoryConfig } from "./memory.js";
import { ChromaDBLifecycle } from "./chroma-lifecycle.js";
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
let memoryHostAnnounceInterval: ReturnType<typeof setInterval> | null = null;
let pruneInterval: ReturnType<typeof setInterval> | null = null;

const store: MeshStore = {
  peers: new Map(),
  broadcastHistory: [],
  agentName: "", // set during extension init after flag is read
  autoReplyAll: false, // when true, all incoming messages auto-reply without LLM
};

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
      notify(pi, `Peer connected: ${ev.peerId}`);
      break;
    }

    case "peer:disconnected": {
      const p = store.peers.get(ev.peerId);
      if (p) { p.status = "disconnected"; p.disconnectedAt = Date.now(); }
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

  // 1. Session lifecycle: start node
  pi.on("session_start", async (_event, ctx) => {
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
      // Fixes H4: concurrent onRequest calls no longer race on turn_end.
      // Uses a single global turn_end listener + activeRequest flag instead
      // of per-request register/off (pi.off doesn't exist in the ExtensionAPI).

      const REQUEST_TIMEOUT_MS = 60_000;
      const MAX_QUEUE_SIZE = 50;

      type PendingRequest = {
        peerId: string;
        request: import("./types.js").AgentRequest;
        resolve: (text: string) => void;
        timer: ReturnType<typeof setTimeout>;
        /** Set to true when the timeout fires before the entry reaches the LLM. */
        timedOut: boolean;
        /** Auto-retrieved memory context injected into the user message. */
        memoryContext?: string;
      };

      const requestQueue: PendingRequest[] = [];
      let activeRequest: PendingRequest | null = null;

      /** Extract assistant text from a turn_end event. */
      function extractResponseText(msg: any): string {
        if (!msg || msg.role !== "assistant") return "[no assistant response]";
        if (typeof msg.content === "string") return msg.content || "[empty response]";
        if (Array.isArray(msg.content)) {
          return msg.content
            .filter((c: any) => c.type === "text")
            .map((c: any) => c.text)
            .join("\n") || "[empty response]";
        }
        return "[non-text response]";
      }

      /** Dequeue and send the next pending request to the LLM. */
      function advanceQueue() {
        // Already waiting for the current request — the turn_end listener
        // will call this again after it resolves.
        if (activeRequest) return;

        // Discard entries whose timeouts already fired (they resolved via timeout)
        while (requestQueue.length > 0 && requestQueue[0].timedOut) {
          requestQueue.shift();
        }

        if (requestQueue.length === 0) return;

        activeRequest = requestQueue.shift()!;

        // Build the user message with optional memory context
        let userMessage = `[Mesh message from ${activeRequest.request.fromAgent}]\n\n${activeRequest.request.message}`;
        if (activeRequest.memoryContext) {
          userMessage = activeRequest.memoryContext + "\n\n" + userMessage;
        }

        pi.sendUserMessage(userMessage, { deliverAs: "steer" });
      }

      // ONE global turn_end listener — registered once, never removed
      pi.on("turn_end", (event) => {
        if (!activeRequest) return;
        const req = activeRequest;
        activeRequest = null;
        clearTimeout(req.timer);
        const responseText = extractResponseText(event.message);
        req.resolve(responseText);

        // Auto-save exchange to memory (fire-and-forget, non-blocking)
        if (agentMemory) {
          agentMemory.store({
            peerId: req.peerId,
            key: "exchange",
            value: `[Request from ${req.request.fromAgent}] ${req.request.message}\n[Response] ${responseText}`,
            metadata: { type: "conversation_turn", requestId: req.request.requestId },
          }).catch((err) => {
            console.warn("[pi-libp2p-mesh] exchange auto-save failed:", (err as Error).message);
          });
        }

        advanceQueue();
      });

      // Incoming LLM-forward requests (autoReply !== true) — enqueue into FIFO
      meshProtocols.onRequest = async (peerId, request) => {
        // If global auto-reply-all is on, echo without LLM
        if (store.autoReplyAll) {
          return `[auto-reply-all] Received: "${request.message}"`;
        }

        // Backpressure: reject the request immediately if queue is full
        if (requestQueue.length >= MAX_QUEUE_SIZE) {
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

        return new Promise<string>((resolve) => {
          const entry: PendingRequest = {
            peerId,
            request,
            resolve,
            timer: undefined as any,
            timedOut: false,
            memoryContext,
          };

          entry.timer = setTimeout(() => {
            // Mark as timed out so advanceQueue() will skip it
            entry.timedOut = true;
            resolve(
              `[timeout] Agent did not respond within ${REQUEST_TIMEOUT_MS / 1000}s to: "${request.message}"`,
            );
          }, REQUEST_TIMEOUT_MS);

          requestQueue.push(entry);
          advanceQueue();
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

      notify(pi, `Mesh node started as "${config.agentName}" (${meshNode.peerId})`);

      // ── ChromaDB Host Election ───────────────────────────────────────
      // First node to start becomes the host. Others connect to it.
      // Discovery is via GossipSub on the "pi-memory-host" topic.
      const defaultHost = config.chromaHost ?? "localhost";
      const defaultPort = config.chromaPort ?? 8000;
      let resolvedHost = defaultHost;
      let resolvedPort = defaultPort;
      let isMemoryHost = false;
      let hostDiscovered = false;

      // Resolve the local IP we'll advertise if we become the host
      const localIp = resolveLocalIp(meshNode.libp2p);

      // Subscribe to host announcements from other peers
      meshProtocols.subscribeRawTopic<import("./types.js").MemoryHostAnnouncement>(
        "pi-memory-host",
        (announcement) => {
          hostDiscovered = true;
          resolvedHost = announcement.host;
          resolvedPort = announcement.port;
          notify(pi, `Discovered ChromaDB host: ${announcement.fromAgent} at ${announcement.host}:${announcement.port}`);
        },
      );

      chromaLifecycle = new ChromaDBLifecycle({
        host: defaultHost,
        port: defaultPort,
        token: config.chromaToken,
        dataPath: config.chromaDataPath,
      });

      // First check if ChromaDB is already running locally
      const localRunning = await chromaLifecycle.isRunning();

      if (localRunning) {
        // ChromaDB already on localhost — we're the host. Announce it.
        resolvedHost = localIp ?? defaultHost;
        resolvedPort = defaultPort;
        isMemoryHost = true;
        notify(pi, `ChromaDB already running locally — announcing as host at ${resolvedHost}:${resolvedPort}`);
        const announcement: import("./types.js").MemoryHostAnnouncement = {
          type: "memory:host",
          host: resolvedHost,
          port: resolvedPort,
          fromAgent: store.agentName,
          fromPeerId: meshNode.peerId,
          timestamp: Date.now(),
        };
        await meshProtocols.publishRawTopic("pi-memory-host", announcement);
      } else {
        // No local ChromaDB — wait briefly for an existing host announcement
        await new Promise((r) => setTimeout(r, hostDiscovered ? 0 : 2500));

        if (hostDiscovered) {
          notify(pi, `Connecting to mesh ChromaDB host at ${resolvedHost}:${resolvedPort}`);
        } else {
          // No host found — we become the host
          notify(pi, `No ChromaDB host found — becoming the host…`);
          const started = await chromaLifecycle.ensureRunning();
          if (started) {
            resolvedHost = localIp ?? defaultHost;
            resolvedPort = defaultPort;
            isMemoryHost = true;
            const announcement: import("./types.js").MemoryHostAnnouncement = {
              type: "memory:host",
              host: resolvedHost,
              port: resolvedPort,
              fromAgent: store.agentName,
              fromPeerId: meshNode.peerId,
              timestamp: Date.now(),
            };
            await meshProtocols.publishRawTopic("pi-memory-host", announcement);
          } else {
            console.warn("[pi-libp2p-mesh] Failed to start ChromaDB locally — memory tools disabled");
          }
        }
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
    // Stop background pruning
    if (pruneInterval) {
      clearInterval(pruneInterval);
      pruneInterval = null;
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

      const lines = peers.map((p) => {
        const icon = p.status === "connected" ? "🟢" : "🔴";
        const name = p.agentName ?? "unknown";
        const age = Math.round((Date.now() - p.discoveredAt) / 1000);
        return `${icon} ${name} — ${p.id.slice(0, 16)}… (${p.status}, ${age}s ago)`;
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

      const lines = peers.map((p) => {
        const icon = p.status === "connected" ? "🟢" : "🔴";
        const name = p.agentName ?? "unknown";
        const age = Math.round((Date.now() - p.discoveredAt) / 1000);
        return `${icon} ${name} — ${p.id} (${p.status}, ${age}s ago)`;
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
}

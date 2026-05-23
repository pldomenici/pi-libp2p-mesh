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
import { registerMeshTools, registerMemoryTools, setMeshProtocols, listPeers, pruneAllDisconnected, pruneStalePeers, recordBroadcast, type MeshStore } from "./tools.js";
import { MeshDatabase, DEFAULT_DB_PATH } from "./db.js";
import os from "node:os";

// ── Shared State ─────────────────────────────────────────────────────────────
// This singleton is re-created on each extension load (session reload).
// Peer state is re-discovered on each session start.

let meshNode: MeshNode | null = null;
let meshProtocols: MeshProtocols | null = null;
let pruneInterval: ReturnType<typeof setInterval> | null = null;

/** Guard to prevent db:updated broadcast loops. */
let _suppressDbNotify = false;

// Placeholder — the real store is created in session_start after DB is opened
let store: MeshStore = {
  db: null as unknown as MeshDatabase,
  agentName: "",
  autoReplyAll: false,
  notifyDbChanged: undefined,
};

// ── Helpers ──────────────────────────────────────────────────────────────────

function notify(_pi: ExtensionAPI, msg: string, level: "info" | "warning" | "error" = "info") {
  console.log(`[pi-libp2p-mesh] ${level}: ${msg}`);
}

function buildConfig(pi: ExtensionAPI): MeshConfig {
  return {
    agentName: store.agentName,
    enableMdns: true,
    enableDht: pi.getFlag("mesh-enable-dht") as boolean,
    gossipTopic: (pi.getFlag("mesh-gossip-topic") as string) || "pi-broadcast",
    listenPorts: { tcp: 0, ws: 0 },
  };
}

// ── DB Notification ──────────────────────────────────────────────────────────

/**
 * Broadcast a compact "db:updated" notification so peers know which table
 * changed and can re-read from the shared-memory DB without re-requesting
 * all data over the wire.
 */
async function notifyDbChanged(
  table: "peers" | "broadcasts" | "messages" | "kv" | "memories",
  affectedPeerId?: string,
) {
  if (_suppressDbNotify || !meshProtocols) return;
  _suppressDbNotify = true;
  try {
    await meshProtocols.broadcast({
      fromAgent: store.agentName,
      message: `db:updated:${table}${affectedPeerId ? `:${affectedPeerId.slice(0, 12)}` : ""}`,
      type: "db:updated",
      table,
      affectedPeerId,
    } satisfies Omit<import("./types.js").BroadcastMessage, "fromPeerId" | "timestamp">);
  } catch {
    // Best-effort — don't block on notification failure
  } finally {
    _suppressDbNotify = false;
  }
}

/**
 * Read the updated table from the shared DB when a `db:updated` broadcast
 * is received, and format a human-readable summary for the LLM.
 */
function buildDbUpdatedMessage(msg: import("./types.js").BroadcastMessage): string {
  const table = msg.table!;
  const affectedPeerId = msg.affectedPeerId;

  switch (table) {
    case "peers": {
      if (affectedPeerId) {
        const peer = store.db.getPeer(affectedPeerId);
        if (peer) {
          const icon = peer.status === "connected" ? "🟢" : "🔴";
          return `[DB update: ${msg.fromAgent} wrote to peers table]\n${icon} **${peer.agentName ?? "unknown"}** (${peer.id}) — ${peer.status}\nAddresses: ${peer.addresses.join(", ") || "none"}`;
        }
      }
      // Full peer table summary
      const all = store.db.getAllPeers();
      const connected = all.filter(p => p.status === "connected").length;
      return `[DB update: ${msg.fromAgent} wrote to peers table]\n${connected}/${all.length} peers in DB: ${all.map(p => (p.status === "connected" ? "🟢" : "🔴") + " " + (p.agentName ?? "unknown")).join(", ")}`;
    }

    case "broadcasts": {
      const recent = store.db.getBroadcasts(5);
      if (recent.length === 0) return `[DB update: ${msg.fromAgent} wrote to broadcasts table]\nNo broadcasts in history.`;
      return `[DB update: ${msg.fromAgent} wrote to broadcasts table]\nLast ${recent.length} broadcasts:\n${recent.map(b => `  • ${b.fromAgent}: "${b.message.slice(0, 80)}…"`).join("\n")}`;
    }

    case "messages": {
      const recent = store.db.getMessages(5);
      if (recent.length === 0) return `[DB update: ${msg.fromAgent} wrote to messages table]\nNo messages in history.`;
      return `[DB update: ${msg.fromAgent} wrote to messages table]\nLast ${recent.length} messages:\n${recent.map(m => `  • ${m.direction} ${m.from_agent}: "${m.message.slice(0, 80)}…"`).join("\n")}`;
    }

    case "memories": {
      const count = store.db.getMemoriesCount();
      if (affectedPeerId) {
        const peer = store.db.getPeer(affectedPeerId);
        const name = peer?.agentName ?? "unknown";
        const peerMemories = store.db.recallByPeer(affectedPeerId);
        const summaries = peerMemories.slice(0, 3).map(m => `  • [${m.key}] ${m.value.slice(0, 80)}…`).join("\n");
        return `[DB update: ${msg.fromAgent} wrote to memories table]\n**${name}** has ${peerMemories.length} memory/memories\n${summaries || "  (none)"}`;
      }
      return `[DB update: ${msg.fromAgent} wrote to memories table]\nTotal memories: ${count}`;
    }

    default:
      return `[DB update: ${msg.fromAgent} wrote to ${table}]`;
  }
}

// ── Event Handler ────────────────────────────────────────────────────────────

function handleNodeEvent(pi: ExtensionAPI, ev: MeshNodeEvent) {
  switch (ev.type) {
    case "peer:discovered": {
      const existing = store.db.getPeer(ev.peer.id);
      if (existing) {
        // Merge addresses (discovery may fire after connect/identify)
        const existingAddrs = new Set(existing.addresses);
        for (const addr of ev.peer.addresses) existingAddrs.add(addr);
        existing.addresses = [...existingAddrs];
        // Don't downgrade from connected to disconnected if we already have an active connection
        existing.discoveredAt = ev.peer.discoveredAt;
        store.db.upsertPeer(existing);
      } else {
        store.db.upsertPeer(ev.peer);
      }
      if (meshProtocols) meshProtocols.handlePeerDiscovered(ev.peer);
      notifyDbChanged("peers", ev.peer.id);
      notify(pi, `Peer discovered: ${ev.peer.id} (${ev.peer.addresses.join(", ")})`);
      break;
    }

    case "peer:connected": {
      let p = store.db.getPeer(ev.peerId);
      if (!p) {
        // Inbound connection before mDNS discovery — create a placeholder
        p = {
          id: ev.peerId,
          addresses: [],
          status: "connected",
          discoveredAt: Date.now(),
        };
      }
      p.status = "connected";
      p.disconnectedAt = undefined;
      store.db.upsertPeer(p);
      notifyDbChanged("peers", ev.peerId);
      notify(pi, `Peer connected: ${ev.peerId}`);
      break;
    }

    case "peer:disconnected": {
      const p = store.db.getPeer(ev.peerId);
      if (p) {
        p.status = "disconnected";
        p.disconnectedAt = Date.now();
        store.db.upsertPeer(p);
        notifyDbChanged("peers", ev.peerId);
      }
      notify(pi, `Peer disconnected: ${ev.peerId}`);
      break;
    }

    case "peer:identified": {
      let p = store.db.getPeer(ev.peerId);
      if (!p) {
        // Identify before discovery — create a placeholder
        p = {
          id: ev.peerId,
          addresses: [],
          status: "connected",
          discoveredAt: Date.now(),
        };
      }
      if (ev.agentName) {
        p.agentName = ev.agentName;
        store.db.upsertPeer(p);
        notifyDbChanged("peers", ev.peerId);
        notify(pi, `Peer identified: ${ev.peerId.slice(0, 12)}… as "${ev.agentName}"`);
      }
      break;
    }

    case "message":
      // Log incoming message to DB
      store.db.logMessage({
        direction: "incoming",
        peerId: ev.request.fromPeerId,
        requestId: ev.request.requestId,
        fromAgent: ev.request.fromAgent,
        message: ev.request.message,
      });
      notifyDbChanged("messages", ev.request.fromPeerId);
      notify(pi, `Message from ${ev.request.fromAgent}: ${ev.request.message.slice(0, 120)}`);
      break;

    case "broadcast":
      recordBroadcast(store, ev.message);
      // Don't re-notify for broadcasts — that would create an infinite loop.
      // The onBroadcast handler below handles db:updated notifications.
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
  pi.registerFlag("mesh-db-path", {
    description: `Path to the SQLite database file (default: ${DEFAULT_DB_PATH})`,
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
    const agentName = flagName || envName || `pi-${hostname}`;

    // ── Open SQLite database (WAL mode, shared memory) ────────────────────
    const dbPath = (pi.getFlag("mesh-db-path") as string) || DEFAULT_DB_PATH;
    const db = new MeshDatabase(dbPath, agentName);
    store = { db, agentName, autoReplyAll: false, notifyDbChanged };

    // Mark peers from previous sessions as disconnected.
    // Without this, restarted agents with new PeerIds leave stale
    // "connected" entries in the DB forever.
    const disconnected = db.disconnectPeersFromOtherSessions();
    if (disconnected > 0) {
      notify(pi, `Marked ${disconnected} peer(s) from previous sessions as disconnected`);
    }

    const config = buildConfig(pi);

    try {
      meshNode = await MeshNode.create(config);
      meshProtocols = new MeshProtocols(meshNode.libp2p, config);

      // Wire protocols into tools so mesh_send / mesh_broadcast work
      setMeshProtocols(meshProtocols);

      // Incoming direct messages — forward via pi's event bus.
      // Logging is handled in handleNodeEvent (single source of truth).
      meshProtocols.onMessage = (_peerId, request) => {
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
        pi.sendUserMessage(
          `[Mesh message from ${activeRequest.request.fromAgent}]\n\n${activeRequest.request.message}`,
          { deliverAs: "steer" },
        );
      }

      // ONE global turn_end listener — registered once, never removed
      pi.on("turn_end", (event) => {
        if (!activeRequest) return;
        const req = activeRequest;
        activeRequest = null;
        clearTimeout(req.timer);
        req.resolve(extractResponseText(event.message));
        advanceQueue();
      });

      // Incoming LLM-forward requests (autoReply !== true) — enqueue into FIFO
      meshProtocols.onRequest = (peerId, request) => {
        // If global auto-reply-all is on, echo without LLM
        if (store.autoReplyAll) {
          return Promise.resolve(
            `[auto-reply-all] Received: "${request.message}"`,
          );
        }

        // Backpressure: reject the request immediately if queue is full
        if (requestQueue.length >= MAX_QUEUE_SIZE) {
          return Promise.resolve(
            `[queue-full] Agent request queue is full (max ${MAX_QUEUE_SIZE}). Please retry later.`,
          );
        }

        return new Promise<string>((resolve) => {
          const entry: PendingRequest = {
            peerId,
            request,
            resolve,
            timer: undefined as any,
            timedOut: false,
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
        // db:updated — a peer wrote to the shared DB. Read the updated table
        // and notify the LLM without re-broadcasting (avoids infinite loop).
        if (msg.type === "db:updated" && msg.table) {
          if (!store.autoReplyAll) {
            pi.sendUserMessage(
              buildDbUpdatedMessage(msg),
              { deliverAs: "steer" },
            );
          }
          return; // Don't record as a broadcast — prevents loops
        }

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
      };

      // Forward node events into our handler
      meshNode.onEvent((ev) => handleNodeEvent(pi, ev));

      await meshNode.start();

      notify(pi, `Mesh node started as "${config.agentName}" (${meshNode.peerId})`);

      // Background stale-peer pruning — runs every 30s to keep the peer
      // table clean without explicit LLM-triggered prune commands.
      pruneInterval = setInterval(() => {
        const removed = pruneStalePeers(store);
        if (removed > 0) {
          notify(pi, `Background prune: removed ${removed} stale peer(s)`);
        }
      }, 30_000);

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
    if (meshProtocols) {
      await meshProtocols.stop();
    }
    if (meshNode) {
      await meshNode.stop();
    }
    meshNode = null;
    meshProtocols = null;
    setMeshProtocols(null);

    // WAL checkpoint + close database
    if (store.db) {
      store.db.checkpoint();
      store.db.close();
    }

    notify(pi, "Mesh node stopped");
  });

  // 3. Register mesh tools
  registerMeshTools(pi, store);
  registerMemoryTools(pi, store);

  // 4. Register commands for manual control
  pi.registerCommand("auto-reply", {
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
          `Usage: /auto-reply [on|off] — current: ${store.autoReplyAll ? "ON" : "off"}`,
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
        ctx.ui.notify("No peers discovered. Use mesh_discover to scan.", "info");
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
    description: "Scan for new peers on the P2P mesh network",
    handler: async (_args, ctx) => {
      if (!meshNode) {
        ctx.ui.notify("Mesh node not running", "warning");
        return;
      }

      ctx.ui.notify("Scanning network for new peers…", "info");

      const { peers, connected, total } = listPeers(store);
      const newPeers = peers.filter(
        (p) => Date.now() - p.discoveredAt < 10_000,
      );

      if (total === 0) {
        ctx.ui.notify(
          "No peers discovered. Ensure other pi agents with pi-libp2p-mesh are running on the same network.",
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
        `${connected}/${total} peers (${newPeers.length} recently discovered):\n${lines.join("\n")}`,
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

      const before = store.db.getAllPeers().length;
      const removed = pruneAllDisconnected(store);
      const after = store.db.getAllPeers().length;
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

  pi.registerCommand("mesh-memory", {
    description: "Store, recall, or summarize agent memories (persistent across restarts)",
    handler: async (args, ctx) => {
      if (!store.db) {
        ctx.ui.notify("Database not available", "warning");
        return;
      }

      const parts = args.trim().split(/\s+/);
      const subcmd = parts[0]?.toLowerCase();

      switch (subcmd) {
        case "store": {
          // mesh-memory store <key> <value> [--peer <peerId>] [--tag <tag>] [--importance <1-5>]
          const key = parts[1];
          const valueIdx = args.indexOf(parts[1] ?? "") + (parts[1]?.length ?? 0) + 1;
          const valueEnd = args.indexOf(" --", valueIdx);
          const value = valueEnd === -1 ? args.slice(valueIdx).trim() : args.slice(valueIdx, valueEnd).trim();
          if (!key || !value) {
            ctx.ui.notify("Usage: /mesh-memory store <key> <value> [--peer <peerId>] [--tag <tag>] [--importance <1-5>]", "warning");
            return;
          }

          const peerFlag = args.match(/--peer\s+(\S+)/);
          const tagFlag = args.match(/--tag\s+(\S+)/);
          const impFlag = args.match(/--importance\s+(\d)/);
          const tags = tagFlag ? tagFlag[1].split(",") : [];
          const importance = impFlag ? Math.min(Math.max(parseInt(impFlag[1], 10), 1), 5) : 1;

          const stored = store.db.storeMemory({
            peerId: peerFlag?.[1],
            agentName: undefined,
            key,
            value,
            tags,
            importance,
          });
          ctx.ui.notify(`🧠 Memory stored (id=${stored.id}) [${key}]: ${value.slice(0, 80)}…`, "info");
          break;
        }

        case "recall":
        case "list": {
          // mesh-memory recall [--peer <peerId>] [--key <key>] [--agent <name>] [--search <query>]
          const peerFilter = args.match(/--peer\s+(\S+)/);
          const keyFilter = args.match(/--key\s+(\S+)/);
          const agentFilter = args.match(/--agent\s+(\S+)/);
          const searchQuery = args.match(/--search\s+(.+)/);

          let memories = store.db.getAllMemories(50);
          if (peerFilter) memories = store.db.recallByPeer(peerFilter[1]);
          else if (keyFilter) memories = store.db.recallByKey(keyFilter[1]);
          else if (agentFilter) memories = store.db.recallByAgent(agentFilter[1]);
          else if (searchQuery) memories = store.db.searchMemories(searchQuery[1], 50);

          if (memories.length === 0) {
            ctx.ui.notify("🧠 No memories found.", "info");
            return;
          }

          const lines = memories.slice(0, 20).map((m) => {
            const stars = "⭐".repeat(m.importance);
            const tagHint = m.tags.length ? ` [${m.tags.join(", ")}]` : "";
            return `  ${stars} \`${m.key}\` (id=${m.id})${tagHint}: ${m.value.slice(0, 100)}${m.value.length > 100 ? "…" : ""}`;
          });
          ctx.ui.notify(`🧠 ${memories.length} memory/memories:\n${lines.join("\n")}`, "info");
          break;
        }

        case "summarize": {
          // mesh-memory summarize <agentName> [--key <key>]
          const agent = parts[1];
          if (!agent) {
            ctx.ui.notify("Usage: /mesh-memory summarize <agentName> [--key <key>]", "warning");
            return;
          }
          const key = args.match(/--key\s+(\S+)/)?.[1] ?? "conversation_summary";
          const recentMessages = store.db.getMessages(20);
          const relevant = recentMessages.filter((m) => m.from_agent === agent);
          if (relevant.length === 0) {
            ctx.ui.notify(`No conversations found with ${agent}.`, "warning");
            return;
          }
          const summaryValue = relevant
            .slice(-10)
            .map((m) => `[${new Date(m.timestamp).toISOString().slice(0, 16)}] ${m.direction === "incoming" ? "←" : "→"} ${m.from_agent}: "${m.message.slice(0, 200)}${m.message.length > 200 ? "…" : ""}"`)
            .join("\n");
          const stored = store.db.storeMemory({
            agentName: agent,
            key,
            value: `Recent conversations (${relevant.length} messages):\n${summaryValue}`,
            tags: ["auto-summary"],
            importance: 3,
          });
          ctx.ui.notify(`📝 Summarized ${relevant.length} messages with ${agent} (memory id=${stored.id})`, "info");
          break;
        }

        case "forget":
        case "delete": {
          // mesh-memory forget <id>   or   mesh-memory forget --key <key>
          const id = parseInt(parts[1], 10);
          if (!isNaN(id)) {
            const ok = store.db.forgetMemory(id);
            ctx.ui.notify(ok ? `🧹 Forgotten memory id=${id}.` : `No memory found with id=${id}.`, ok ? "info" : "warning");
          } else {
            const byKey = args.match(/--key\s+(\S+)/)?.[1];
            if (byKey) {
              const deleted = store.db.forgetByKey(byKey);
              ctx.ui.notify(`🧹 Forgotten ${deleted} memory/memories with key "${byKey}".`, "info");
            } else {
              ctx.ui.notify("Usage: /mesh-memory forget <id> | --key <key>", "warning");
            }
          }
          break;
        }

        default: {
          const count = store.db.getMemoriesCount();
          ctx.ui.notify(
            `🧠 Agent Memory — ${count} total\n\nCommands:\n  /mesh-memory store <key> <value> [--peer <id>] [--tag <tag>] [--importance <1-5>]\n  /mesh-memory recall [--peer <id>] [--key <key>] [--agent <name>] [--search <query>]\n  /mesh-memory summarize <agentName> [--key <key>]\n  /mesh-memory forget <id> | --key <key>\n  /mesh-memory list (same as recall)`, "info");
          break;
        }
      }
    },
  });
}

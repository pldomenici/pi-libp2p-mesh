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
 *   The mesh picks up the agent name from the pi config. Per-session settings
 *   are stored via pi.appendEntry() and restored on reload.
 */

import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import type { MeshConfig, MeshNodeEvent } from "./types";
import { MeshNode } from "./node";
import { MeshProtocols } from "./protocols";
import { registerMeshTools, setMeshProtocols, type MeshStore } from "./tools";
import os from "node:os";

// ── Shared State ─────────────────────────────────────────────────────────────
// This singleton is re-created on each extension load (session reload).
// Persisted peer book survives restarts via pi.appendEntry / session_start restore.

let meshNode: MeshNode | null = null;
let meshProtocols: MeshProtocols | null = null;

const store: MeshStore = {
  peers: new Map(),
  broadcastHistory: [],
  agentName: "", // set during extension init after flag is read
};

// ── Helpers ──────────────────────────────────────────────────────────────────

function notify(pi: ExtensionAPI, msg: string, level: "info" | "warn" | "error" = "info") {
  // We access ctx.ui.notify via events; for direct use we fire-and-forget.
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
      break;
    }

    case "message":
      // Incoming direct message — could forward to LLM via pi.sendMessage
      // For now we log; the LLM accesses via tools.
      notify(pi, `Message from ${ev.request.fromAgent}: ${ev.request.message.slice(0, 120)}`);
      break;

    case "broadcast":
      store.broadcastHistory.push(ev.message);
      notify(pi, `Broadcast from ${ev.message.fromAgent}: ${ev.message.message.slice(0, 120)}`);
      break;

    case "error":
      notify(pi, ev.error.message, "error");
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

    try {
      meshNode = await MeshNode.create(config);
      meshProtocols = new MeshProtocols(meshNode.libp2p, config);

      // Wire protocols into tools so mesh_send / mesh_broadcast work
      setMeshProtocols(meshProtocols);

      // Incoming direct messages — forward via pi's event bus
      meshProtocols.onMessage = (_peerId, request) => {
        handleNodeEvent(pi, { type: "message", fromPeerId: request.fromPeerId, request });
      };

      // Incoming LLM-forward requests (autoReply !== true) — inject into pi's LLM
      meshProtocols.onRequest = (peerId, request) => {
        return new Promise<string>((resolve) => {
          let settled = false;

          const timeout = setTimeout(() => {
            if (settled) return;
            settled = true;
            resolve(
              `[timeout] Agent did not respond within 60s to: "${request.message}"`,
            );
          }, 60_000);

          pi.on("turn_end", (event) => {
            if (settled) return;
            settled = true;
            clearTimeout(timeout);

            const msg = event.message;
            if (msg && msg.role === "assistant") {
              const text =
                typeof msg.content === "string"
                  ? msg.content
                  : Array.isArray(msg.content)
                    ? msg.content
                        .filter((c: any) => c.type === "text")
                        .map((c: any) => c.text)
                        .join("\n")
                    : "[non-text response]";
              resolve(text || "[empty response]");
            } else {
              resolve("[no assistant response]");
            }
          });

          // Inject into pi's processing queue
          pi.sendUserMessage(
            `[Mesh message from ${request.fromAgent}]\n\n${request.message}`,
            { deliverAs: "steer" },
          );
        });
      };

      // Incoming broadcasts — record in store and notify
      meshProtocols.onBroadcast = (msg) => {
        handleNodeEvent(pi, { type: "broadcast", message: msg });
      };

      // Forward node events into our handler
      meshNode.onEvent((ev) => handleNodeEvent(pi, ev));

      await meshNode.start();

      notify(pi, `Mesh node started as "${config.agentName}" (${meshNode.peerId})`);

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
    if (meshProtocols) {
      await meshProtocols.stop();
    }
    if (meshNode) {
      await meshNode.stop();
    }
    meshNode = null;
    meshProtocols = null;
    setMeshProtocols(null as any);
    notify(pi, "Mesh node stopped");
  });

  // 3. Register mesh tools
  registerMeshTools(pi, store);

  // 4. Register a command for manual control
  pi.registerCommand("mesh-status", {
    description: "Show mesh network status",
    handler: async (_args, ctx) => {
      if (!meshNode) {
        ctx.ui.notify("Mesh node not running", "warn");
        return;
      }

      const connected = [...store.peers.values()].filter(
        (p) => p.status === "connected",
      );
      ctx.ui.notify(
        `Mesh: ${connected.length}/${store.peers.size} peers connected | Node: ${meshNode.peerId}`,
        "info",
      );
    },
  });
}

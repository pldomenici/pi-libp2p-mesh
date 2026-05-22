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
import { v4 as uuid } from "uuid";
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
    case "peer:discovered":
      store.peers.set(ev.peer.id, ev.peer);
      if (meshProtocols) meshProtocols.handlePeerDiscovered(ev.peer);
      notify(pi, `Peer discovered: ${ev.peer.id} (${ev.peer.addresses.join(", ")})`);
      break;

    case "peer:connected": {
      const p = store.peers.get(ev.peerId);
      if (p) p.status = "connected";
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
      const p = store.peers.get(ev.peerId);
      if (p && ev.agentName) {
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
    description: "Agent name for the P2P mesh (default: pi-<hostname>)",
    type: "string",
    default: `pi-${hostname}`,
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

  // Resolve agent name (flag takes priority over UUID fallback)
  const flagName = pi.getFlag("agent-name") as string;
  store.agentName = flagName || `pi-${hostname}-${uuid().slice(0, 8)}`;

  // 1. Session lifecycle: start node
  pi.on("session_start", async (_event, ctx) => {
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

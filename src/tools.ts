/**
 * pi-libp2p-mesh / tools.ts
 *
 * Registers four custom pi tools that expose the libp2p mesh to the LLM:
 *   mesh_list_peers   — enumerate known peers
 *   mesh_send         — send a direct message to a peer
 *   mesh_broadcast    — broadcast via GossipSub
 *   mesh_discover     — scan for new peers
 */

import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { Type } from "typebox";
import { StringEnum } from "@earendil-works/pi-ai";

import type {
  MeshPeer,
  BroadcastMessage,
  MeshSendResult,
  MeshBroadcastResult,
  MeshDiscoverResult,
} from "./types";
import type { MeshProtocols } from "./protocols";

// ── Shared State ─────────────────────────────────────────────────────────────

/**
 * Mutable store shared between the extension entry point and tools.
 * Populated in index.ts, consumed here.
 */
export interface MeshStore {
  peers: Map<string, MeshPeer>;
  broadcastHistory: BroadcastMessage[];
  agentName: string;
}

/** Module-level reference — set by index.ts after session_start. */
let meshProtocols: MeshProtocols | null = null;

/**
 * Wire the active MeshProtocols instance so tools can use it.
 * Called from index.ts after session_start.
 */
export function setMeshProtocols(protocols: MeshProtocols): void {
  meshProtocols = protocols;
}

// ── Tool Registration ────────────────────────────────────────────────────────

/**
 * Register all four mesh tools on the pi extension API.
 */
export function registerMeshTools(pi: ExtensionAPI, store: MeshStore): void {
  // ── mesh_list_peers ─────────────────────────────────────────────────────
  pi.registerTool({
    name: "mesh_list_peers",
    label: "List Mesh Peers",
    description:
      "List all known peers on the P2P mesh network, including their connection status, addresses, and agent names.",
    promptSnippet: "List peers on the libp2p mesh network",
    promptGuidelines: [
      "Use mesh_list_peers to see what other pi agents are available before sending messages on the mesh.",
    ],
    parameters: Type.Object({}),

    async execute() {
      // Prune stale peers (disconnected > 5 minutes, never connected)
      const now = Date.now();
      const STALE_MS = 5 * 60 * 1000;
      for (const [id, peer] of store.peers) {
        if (peer.status === "disconnected") {
          const lastSeen = peer.disconnectedAt ?? peer.discoveredAt;
          if (now - lastSeen > STALE_MS) {
            store.peers.delete(id);
          }
        }
      }

      const peers = [...store.peers.values()];
      const connected = peers.filter((p) => p.status === "connected").length;
      const total = peers.length;

      const text =
        total === 0
          ? "No peers discovered yet. Use mesh_discover to scan the network."
          : `Found ${connected} connected / ${total} total peers:\n\n` +
            peers
              .map((p) => {
                const name = p.agentName ?? "unknown";
                const addrs = p.addresses.join(", ") || "none";
                return `  ${p.status === "connected" ? "🟢" : "🔴"} **${name}** (${p.id.slice(0, 12)}…) — ${addrs}`;
              })
              .join("\n");

      return {
        content: [{ type: "text", text }],
        details: { peers, connected, total },
      };
    },
  });

  // ── mesh_send ───────────────────────────────────────────────────────────
  pi.registerTool({
    name: "mesh_send",
    label: "Send Mesh Message",
    description:
      "Send a prompt or message directly to a specific peer on the P2P mesh network. " +
      "Returns the peer's response. Use mesh_list_peers first to find active peers.",
    promptSnippet: "Send a direct message to a specific peer on the mesh",
    promptGuidelines: [
      "Use mesh_send to ask another pi agent a question or delegate a sub-task on the P2P mesh. Use mesh_list_peers first to find active peer IDs.",
    ],
    parameters: Type.Object({
      peerId: Type.String({
        description: "Target PeerId (base58 string) of the pi agent to message",
      }),
      message: Type.String({
        description: "The prompt, question, or message to send to the peer",
      }),
    }),

    async execute(_toolCallId, params, _signal, onUpdate) {
      if (!meshProtocols) {
        return {
          content: [
            {
              type: "text",
              text: "Mesh is not running. The libp2p node has not been started yet.",
            },
          ],
          details: { error: "mesh not initialized" },
        };
      }

      onUpdate?.({
        content: [{ type: "text", text: `Dialing peer ${params.peerId.slice(0, 12)}…` }],
      });

      try {
        const response = await meshProtocols.sendMessage(params.peerId, {
          protocol: "/pi-agent/0.1.0",
          requestId: crypto.randomUUID?.() ?? `${Date.now()}-${Math.random().toString(36).slice(2, 10)}`,
          fromAgent: store.agentName,
          message: params.message,
        });

        onUpdate?.({
          content: [{ type: "text", text: `Response from ${response.fromAgent}:` }],
        });

        const result: MeshSendResult = {
          peerId: params.peerId,
          agentName: response.fromAgent,
          response,
        };

        const peer = store.peers.get(params.peerId);
        return {
          content: [
            {
              type: "text",
              text: `**Response from ${response.fromAgent}** (${params.peerId.slice(0, 12)}…):\n\n${response.message}` +
                (response.error ? "\n\n⚠️ Peer reported an error." : ""),
            },
          ],
          details: result,
        };
      } catch (err: any) {
        const result: MeshSendResult = {
          peerId: params.peerId,
          response: null,
          error: err.message,
        };
        return {
          content: [
            {
              type: "text",
              text: `Failed to reach peer ${params.peerId.slice(0, 12)}…: ${err.message}`,
            },
          ],
          details: result,
        };
      }
    },
  });

  // ── mesh_broadcast ──────────────────────────────────────────────────────
  pi.registerTool({
    name: "mesh_broadcast",
    label: "Broadcast to Mesh",
    description:
      "Broadcast a message to all connected peers on the P2P mesh network via GossipSub. " +
      "Use this to announce events, share discoveries, or coordinate group actions.",
    promptSnippet: "Broadcast a message to all peers on the mesh",
    promptGuidelines: [
      "Use mesh_broadcast to announce changes, share discoveries, or coordinate with all agents at once. Keep broadcasts concise — all peers receive them.",
    ],
    parameters: Type.Object({
      message: Type.String({
        description: "The message to broadcast to all peers",
      }),
      type: Type.Optional(
        StringEnum(["announce", "query", "response", "event"] as const),
      ),
    }),

    async execute(_toolCallId, params, _signal, onUpdate) {
      if (!meshProtocols) {
        return {
          content: [
            {
              type: "text",
              text: "Mesh is not running. The libp2p node has not been started yet.",
            },
          ],
          details: { error: "mesh not initialized" },
        };
      }

      onUpdate?.({
        content: [{ type: "text", text: "Broadcasting to all peers…" }],
      });

      try {
        const result = await meshProtocols.broadcast({
          fromAgent: store.agentName,
          message: params.message,
          type: params.type,
        });

        // Record in history
        store.broadcastHistory.push({
          fromAgent: store.agentName,
          fromPeerId: "self",
          timestamp: Date.now(),
          message: params.message,
          type: params.type,
        });

        return {
          content: [
            {
              type: "text",
              text: `📡 Broadcast sent on topic "${result.topic}" to approximately ${result.peersReached} peer(s).\nMessage ID: ${result.messageId}`,
            },
          ],
          details: result,
        };
      } catch (err: any) {
        const result: MeshBroadcastResult = {
          topic: "pi-broadcast",
          peersReached: 0,
          messageId: "error",
        };
        return {
          content: [
            {
              type: "text",
              text: `Broadcast failed: ${err.message}`,
            },
          ],
          details: result,
        };
      }
    },
  });

  // ── mesh_discover ───────────────────────────────────────────────────────
  pi.registerTool({
    name: "mesh_discover",
    label: "Discover Mesh Peers",
    description:
      "Actively scan the P2P mesh network for new pi agents. Returns any newly discovered " +
      "peers and their addresses. Peers already in the peer list are included in the total count.",
    promptSnippet: "Scan for new peers on the mesh",
    promptGuidelines: [
      "Use mesh_discover to find new pi agents on the network before using mesh_send or mesh_broadcast.",
    ],
    parameters: Type.Object({}),

    async execute(_toolCallId, _params, _signal, onUpdate) {
      onUpdate?.({
        content: [{ type: "text", text: "Scanning network for new peers…" }],
      });

      // Prune stale peers
      const now = Date.now();
      const STALE_MS = 5 * 60 * 1000;
      for (const [id, peer] of store.peers) {
        if (peer.status === "disconnected") {
          const lastSeen = peer.disconnectedAt ?? peer.discoveredAt;
          if (now - lastSeen > STALE_MS) {
            store.peers.delete(id);
          }
        }
      }

      // mDNS and DHT discovery is continuous. We report the current state,
      // which includes all peers found since the node started (or last /reload).
      const peers = [...store.peers.values()];
      const newPeers = peers.filter(
        (p) => Date.now() - p.discoveredAt < 10_000,
      );

      onUpdate?.({
        content: [
          {
            type: "text",
            text: `Found ${peers.length} known peer(s), ${newPeers.length} recently discovered.`,
          },
        ],
      });

      const result: MeshDiscoverResult = {
        peersFound: peers.length,
        peers,
      };

      const text =
        peers.length === 0
          ? "No peers discovered yet. Ensure other pi agents with pi-libp2p-mesh are running on the same network (mDNS) or configure DHT/bootstrap peers."
          : `Discovered ${peers.length} peer(s):\n\n` +
            peers
              .map((p) => {
                const name = p.agentName ?? "unnamed";
                const age = Math.round((Date.now() - p.discoveredAt) / 1000);
                return `  • **${name}** — ${p.id.slice(0, 12)}… (${p.status}, ${age}s ago)`;
              })
              .join("\n");

      return {
        content: [{ type: "text", text }],
        details: result,
      };
    },
  });
}

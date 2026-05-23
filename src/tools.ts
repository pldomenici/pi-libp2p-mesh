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
import { Type, type TSchema } from "typebox";

/**
 * Create a TypeBox union of string literal types from a const array.
 */
function StringEnum<T extends readonly string[]>(values: T): TSchema {
  return Type.Union(values.map((v) => Type.Literal(v)));
}

import type {
  MeshPeer,
  BroadcastMessage,
  MeshSendResult,
  MeshBroadcastResult,
  MeshDiscoverResult,
} from "./types.js";
import type { MeshProtocols } from "./protocols.js";
import type { MeshDatabase } from "./db.js";

// ── Shared State ─────────────────────────────────────────────────────────────

/**
 * Mutable store shared between the extension entry point and tools.
 * Populated in index.ts, consumed here.
 *
 * Peer and broadcast data is persisted to SQLite via {@link db}.
 */
export interface MeshStore {
  db: MeshDatabase;
  agentName: string;
  /** When true, all incoming messages auto-reply without involving the LLM. */
  autoReplyAll: boolean;
}

/**
 * Push a broadcast onto the store's database-backed history,
 * evicting the oldest entry if the cap is exceeded.
 */
export function recordBroadcast(store: MeshStore, msg: BroadcastMessage): void {
  store.db.recordBroadcast(msg);
}

/** Module-level reference — set by index.ts after session_start. */
let meshProtocols: MeshProtocols | null = null;

/**
 * Wire the active MeshProtocols instance so tools can use it.
 * Called from index.ts after session_start.
 */
export function setMeshProtocols(protocols: MeshProtocols | null): void {
  meshProtocols = protocols;
}

// ── Helpers (shared between tools and commands) ──────────────────────────────

const STALE_PEER_MS = 60 * 1000;

/**
 * Remove stale/disconnected peers using two strategies:
 * 1. Agent-name dedup — when two entries share the same agentName, keep only the
 *    connected one (the disconnected entry is from a previous session that restarted).
 * 2. Time-based — remove peers disconnected for more than STALE_PEER_MS (60s).
 *
 * Returns the number of removed entries.
 */
export function pruneStalePeers(store: MeshStore): number {
  return store.db.pruneDedupByName() + store.db.pruneStale(STALE_PEER_MS);
}

/**
 * Aggressively prune ALL disconnected peers immediately (ignoring time threshold).
 * Returns count of removed entries.
 */
export function pruneAllDisconnected(store: MeshStore): number {
  return store.db.pruneAllDisconnected();
}

export interface PeerListResult {
  peers: MeshPeer[];
  connected: number;
  total: number;
}

/** Get current peer list after pruning stale entries. */
export function listPeers(store: MeshStore): PeerListResult {
  pruneStalePeers(store);
  const peers = store.db.getAllPeers();
  const connected = peers.filter((p) => p.status === "connected").length;
  return { peers, connected, total: peers.length };
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
      const { peers, connected, total } = listPeers(store);

      const text =
        total === 0
          ? "No peers discovered yet. Use mesh_discover to scan the network."
          : `Found ${connected} connected / ${total} total peers:\n\n` +
            peers
              .map((p) => {
                const name = p.agentName ?? "unknown";
                const addrs = p.addresses.join(", ") || "none";
                return `  ${p.status === "connected" ? "🟢" : "🔴"} **${name}** (${p.id}) — ${addrs}`;
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
      autoReply: Type.Optional(
        Type.Boolean({
          description:
            "If true, the receiver auto-replies without involving its LLM. " +
            "Defaults to false (message is forwarded to the receiver's LLM).",
        }),
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
        content: [{ type: "text", text: `Dialing peer ${params.peerId}…` }],
        details: {},
      });

      try {
        const MAX_ATTEMPTS = 2;
        const RETRY_DELAY_MS = 500;
        let response: import("./types.js").AgentResponse;

        for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
          try {
            response = await meshProtocols.sendMessage(params.peerId, {
              protocol: "/pi-agent/0.1.0",
              requestId: crypto.randomUUID?.() ?? `${Date.now()}-${Math.random().toString(36).slice(2, 10)}`,
              fromAgent: store.agentName,
              message: params.message,
              autoReply: params.autoReply,
            });
            break; // success — exit retry loop
          } catch (dialErr: any) {
            if (attempt === MAX_ATTEMPTS) throw dialErr; // last attempt — rethrow
            // Transient failure — wait and retry
            if (onUpdate) {
              onUpdate({
                content: [{ type: "text", text: `Retry ${attempt}/${MAX_ATTEMPTS} after dial failure…` }],
                details: {},
              });
            }
            await new Promise((r) => setTimeout(r, RETRY_DELAY_MS));
          }
        }

        // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
        const finalResponse = response!;

        onUpdate?.({
          content: [{ type: "text", text: `Response from ${finalResponse.fromAgent}:` }],
          details: {},
        });

        const result: MeshSendResult = {
          peerId: params.peerId,
          agentName: finalResponse.fromAgent,
          response: finalResponse,
        };
        return {
          content: [
            {
              type: "text",
              text: `**Response from ${finalResponse.fromAgent}** (${params.peerId}):\n\n${finalResponse.message}` +
                (finalResponse.error ? "\n\n⚠️ Peer reported an error." : ""),
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
              text: `Failed to reach peer ${params.peerId}: ${err.message}`,
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
        details: {},
      });

      try {
        const msgType = params.type as "announce" | "query" | "response" | "event" | undefined;
        const result = await meshProtocols.broadcast({
          fromAgent: store.agentName,
          message: params.message,
          type: msgType,
        });

        // Record in history (capped at MAX_BROADCAST_HISTORY)
        recordBroadcast(store, {
          fromAgent: store.agentName,
          fromPeerId: "self",
          timestamp: Date.now(),
          message: params.message,
          type: msgType,
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
        details: {},
      });

      const { peers } = listPeers(store);
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
        details: {},
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
                return `  • **${name}** — ${p.id} (${p.status}, ${age}s ago)`;
              })
              .join("\n");

      return {
        content: [{ type: "text", text }],
        details: result,
      };
    },
  });

  // ── mesh_prune ────────────────────────────────────────────────────────
  pi.registerTool({
    name: "mesh_prune",
    label: "Prune Stale Peers",
    description:
      "Remove disconnected/stale peers from the peer list. " +
      "Use this to clean up old peer entries from restarted agents that now have new PeerIds.",
    promptSnippet: "Remove stale/disconnected peers from the mesh peer list",
    promptGuidelines: [
      "Use mesh_prune after agents restart (they get new PeerIds) to clean up disconnected entries.",
    ],
    parameters: Type.Object({}),

    async execute() {
      const before = store.db.getAllPeers().length;
      const removed = pruneAllDisconnected(store);
      const after = store.db.getAllPeers().length;
      const { connected } = listPeers(store);

      const text =
        removed === 0
          ? `No stale peers to prune. All ${before} peer(s) are connected.`
          : `🧹 Pruned ${removed} stale peer(s). Before: ${before}, after: ${after} (${connected} connected).`;

      return {
        content: [{ type: "text", text }],
        details: { removed, before, after, connected },
      };
    },
  });
}

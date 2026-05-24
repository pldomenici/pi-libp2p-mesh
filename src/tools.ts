/**
 * pi-libp2p-mesh / tools.ts
 *
 * Registers five custom pi tools that expose the libp2p mesh to the LLM:
 *   mesh_list_peers   — enumerate known peers
 *   mesh_send         — send a direct message to a peer
 *   mesh_broadcast    — broadcast via GossipSub
 *   mesh_discover     — refresh known peer list
 *   mesh_prune        — remove stale/disconnected peers
 */

import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { Type, type TSchema } from "typebox";

/** Create a TypeBox union of string literal types from a const array. */
function StringEnum<T extends readonly string[]>(values: T): TSchema {
  return Type.Union(values.map((v) => Type.Literal(v)));
}

import { v4 as uuidv4 } from "uuid";

import type {
  MeshPeer,
  BroadcastMessage,
  MeshSendResult,
  MeshBroadcastResult,
  MeshDiscoverResult,
} from "./types.js";
import type { MeshProtocols } from "./protocols.js";
import type { AgentMemory } from "./memory.js";

// ── Shared State ─────────────────────────────────────────────────────────────

/**
 * Mutable store shared between the extension entry point and tools.
 * Populated in index.ts, consumed here.
 */
export interface MeshStore {
  peers: Map<string, MeshPeer>;
  broadcastHistory: BroadcastMessage[];
  agentName: string;
  /** When true, all incoming messages auto-reply without involving the LLM. */
  autoReplyAll: boolean;
}

/** M3: Maximum number of broadcast entries to retain before evicting oldest. */
export const MAX_BROADCAST_HISTORY = 200;

/**
 * Push a broadcast onto the store's history, evicting the oldest entry if
 * the cap is exceeded (prevents unbounded memory growth).
 */
export function recordBroadcast(store: MeshStore, msg: BroadcastMessage): void {
  store.broadcastHistory.push(msg);
  while (store.broadcastHistory.length > MAX_BROADCAST_HISTORY) {
    store.broadcastHistory.shift();
  }
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

/** Module-level reference to the memory store — set by index.ts after session_start. */
let agentMemory: AgentMemory | null = null;

/**
 * Wire the active AgentMemory instance so tools can use it.
 * Called from index.ts after session_start.
 */
export function setAgentMemory(memory: AgentMemory | null): void {
  agentMemory = memory;
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
  const now = Date.now();
  let removed = 0;

  // Strategy 1: agent-name dedup — same name, keep connected, remove disconnected
  const byName = new Map<string, MeshPeer[]>();
  for (const [, peer] of store.peers) {
    if (peer.agentName) {
      const entries = byName.get(peer.agentName) ?? [];
      entries.push(peer);
      byName.set(peer.agentName, entries);
    }
  }
  for (const [, entries] of byName) {
    if (entries.length > 1) {
      const connected = entries.filter((p) => p.status === "connected");
      if (connected.length > 0) {
        // Keep connected entry, remove disconnected ones with same name
        for (const entry of entries) {
          if (entry.status === "disconnected") {
            store.peers.delete(entry.id);
            removed++;
          }
        }
      }
    }
  }

  // Strategy 2: time-based cleanup
  for (const [id, peer] of store.peers) {
    if (peer.status === "disconnected") {
      const lastSeen = peer.disconnectedAt ?? peer.discoveredAt;
      if (now - lastSeen > STALE_PEER_MS) {
        store.peers.delete(id);
        removed++;
      }
    }
  }

  return removed;
}

/**
 * Aggressively prune ALL disconnected peers immediately (ignoring time threshold).
 * Returns count of removed entries.
 */
export function pruneAllDisconnected(store: MeshStore): number {
  let removed = 0;
  for (const [id, peer] of store.peers) {
    if (peer.status === "disconnected") {
      store.peers.delete(id);
      removed++;
    }
  }
  return removed;
}

export interface PeerListResult {
  peers: MeshPeer[];
  connected: number;
  total: number;
}

/** Get current peer list after pruning stale entries. */
export function listPeers(store: MeshStore): PeerListResult {
  pruneStalePeers(store);
  const peers = [...store.peers.values()];
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
          ? "No peers discovered yet. Peer discovery runs automatically via mDNS in the background — ensure other pi agents with pi-libp2p-mesh are running on the same network."
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
              requestId: uuidv4(),
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

        // Auto-save outgoing exchange to memory (fire-and-forget — don't block the LLM)
        if (agentMemory) {
          agentMemory.store({
            peerId: params.peerId,
            key: "exchange",
            value: `[Sent] ${params.message}\n[Response from ${finalResponse.fromAgent}] ${finalResponse.message}`,
            metadata: { type: "conversation_turn" },
          }).catch((err) => {
            console.warn("[pi-libp2p-mesh] outgoing exchange auto-save failed:", (err as Error).message);
          });
        }

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
  //
  // NOTE: This tool does NOT trigger active network discovery (libp2p's mDNS
  // and DHT run autonomously and cannot be triggered on-demand). Instead, it
  // refreshes the peer list by pruning stale entries and reporting what's
  // currently known — useful after peers have restarted or gone offline.
  //
  pi.registerTool({
    name: "mesh_discover",
    label: "Discover Mesh Peers",
    description:
      "Refresh the known peer list on the P2P mesh network. Prunes stale/disconnected " +
      "peers and returns all currently known peers. Peer discovery happens automatically " +
      "via mDNS (local network) — this tool surfaces what has already been discovered.",
    promptSnippet: "Refresh known peers on the mesh",
    promptGuidelines: [
      "Use mesh_discover to refresh the peer list and prune stale entries before using mesh_send or mesh_broadcast. Note: discovery happens automatically via mDNS in the background — this tool does not trigger network scans.",
    ],
    parameters: Type.Object({}),

    async execute(_toolCallId, _params, _signal, onUpdate) {
      onUpdate?.({
        content: [{ type: "text", text: "Refreshing peer list and pruning stale entries…" }],
        details: {},
      });

      // Prune stale peers first so the view is fresh
      const pruned = pruneAllDisconnected(store);

      const { peers, connected, total } = listPeers(store);

      onUpdate?.({
        content: [
          {
            type: "text",
            text: `${total} known peer(s) (${connected} connected, ${pruned} stale pruned).`,
          },
        ],
        details: {},
      });

      const result: MeshDiscoverResult = {
        peersFound: total,
        peers,
      };

      const text =
        total === 0
          ? "No peers known. Peer discovery via mDNS runs in the background — ensure other pi agents with pi-libp2p-mesh are running on the same network."
          : `${connected}/${total} peer(s):\n\n` +
            peers
              .map((p) => {
                const name = p.agentName ?? "unnamed";
                const age = Math.round((Date.now() - p.discoveredAt) / 1000);
                return `  ${p.status === "connected" ? "🟢" : "🔴"} **${name}** — ${p.id} (${p.status}, ${age}s ago)`;
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
      const before = store.peers.size;
      const removed = pruneAllDisconnected(store);
      const after = store.peers.size;
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

// ── Memory Tool Registration ────────────────────────────────────────────────

/**
 * Register four memory tools on the pi extension API:
 *   memory_store   — save a key-value memory entry for a peer
 *   memory_recall  — recall memories by peer and/or key
 *   memory_search  — semantic search across stored memories
 *   memory_keys    — list keys stored for a peer
 */
export function registerMemoryTools(pi: ExtensionAPI, _store: MeshStore): void {
  // ── memory_store ───────────────────────────────────────────────────────
  pi.registerTool({
    name: "memory_store",
    label: "Store Memory",
    description:
      "Save a key-value memory entry associated with a specific peer. " +
      "Use this to remember facts, decisions, preferences, or context about peers. " +
      "Each call appends a new entry — nothing is overwritten. See AGENT-MEMORY.md for usage patterns.",
    promptSnippet: "Save a key-value memory entry for a peer",
    promptGuidelines: [
      "Use memory_store to remember facts, decisions, preferences, or context about peers. " +
        "Save key information after each meaningful conversation turn — especially when a peer " +
        "shares preferences, makes decisions, or provides important context. " +
        "See AGENT-MEMORY.md for usage patterns and anti-patterns.",
    ],
    parameters: Type.Object({
      peerId: Type.String({ description: "Peer this memory is about" }),
      key: Type.String({
        description:
          'Category/name (e.g., "project_context", "prefs", "decision"). Use short, semantic, reusable keys.',
      }),
      value: Type.String({ description: "The content to remember" }),
      metadata: Type.Optional(
        Type.Record(Type.String(), Type.Union([Type.String(), Type.Number()])),
      ),
    }),

    async execute(_toolCallId: string, params: any, _signal?: AbortSignal) {
      if (!agentMemory) {
        return {
          content: [
            { type: "text", text: "Memory is not available. ChromaDB may not be running." } as const,
          ],
          details: { stored: false, peerId: "", key: "", error: "memory not available" },
        };
      }

      try {
        await agentMemory.store({
          peerId: params.peerId as string,
          key: params.key as string,
          value: params.value as string,
          metadata: params.metadata as Record<string, string | number> | undefined,
        });

        return {
          content: [
            {
              type: "text",
              text: `💾 Stored **${params.key}** for peer ${(params.peerId as string).slice(0, 12)}…`,
            } as const,
          ],
          details: { stored: true, peerId: params.peerId as string, key: params.key as string },
        };
      } catch (err: any) {
        return {
          content: [{ type: "text", text: `Failed to store memory: ${err.message}` } as const],
          details: { stored: false, peerId: "", key: "", error: err.message } as any,
        };
      }
    },
  });

  // ── memory_recall ──────────────────────────────────────────────────────
  pi.registerTool({
    name: "memory_recall",
    label: "Recall Memory",
    description:
      "Recall memory entries for a peer by key. At least one of peerId or key is required. " +
      "Returns entries newest first. Values are truncated unless fullText=true. " +
      "Auto-retrieve already injects the latest exchange + 3 search results on incoming messages.",
    promptSnippet: "Recall memories for a peer by key",
    promptGuidelines: [
      "Use memory_recall when preparing to interact with a peer to check what you already know about them. " +
        "Use mesh_list_peers first to get active peer IDs. " +
        "Auto-retrieve already injects the latest exchange + 3 search results, " +
        "so use this for deeper context or specific key lookups.",
    ],
    parameters: Type.Object({
      peerId: Type.Optional(
        Type.String({ description: "Filter by peer (required if key is omitted)" }),
      ),
      key: Type.Optional(
        Type.String({ description: "Filter by key (required if peerId is omitted)" }),
      ),
      limit: Type.Optional(
        Type.Number({ description: "Max results (default 10, hard max 50)" }),
      ),
      fullText: Type.Optional(
        Type.Boolean({
          description: "Return full untruncated values (default false)",
        }),
      ),
    }),

    async execute(_toolCallId: string, params: any, _signal?: AbortSignal) {
      if (!agentMemory) {
        return {
          content: [{ type: "text", text: "Memory is not available. ChromaDB may not be running." } as const],
          details: { entries: [] as any[], count: 0, error: "memory not available" } as any,
        };
      }

      if (!params.peerId && !params.key) {
        return {
          content: [{ type: "text", text: "At least one of peerId or key is required for memory_recall." } as const],
          details: { entries: [] as any[], count: 0, error: "missing filter" } as any,
        };
      }

      try {
        const entries = await agentMemory.get(
          params.peerId as string | undefined,
          params.key as string | undefined,
          { limit: params.limit as number | undefined, fullText: params.fullText as boolean | undefined },
        );

        if (entries.length === 0) {
          const scope = params.peerId
            ? `peer ${(params.peerId as string).slice(0, 12)}…`
            : `key "${params.key}"`;
          return {
            content: [{ type: "text", text: `No memories found for ${scope}.` } as const],
            details: { entries: [] as any[], count: 0 } as any,
          };
        }

        const peerLabel = params.peerId
          ? `peer ${(params.peerId as string).slice(0, 12)}…`
          : "all peers";
        const keyLabel = params.key ? ` key "${params.key}"` : "";
        const lines = entries.map((e) => {
          const age = Math.round((Date.now() - e.timestamp) / 60_000);
          const ageStr = age < 1 ? "just now" : age < 60 ? `${age}m ago` : `${Math.round(age / 60)}h ago`;
          return `🔑 **${e.key}** (${ageStr})\n${e.value}\n`;
        });

        return {
          content: [
            {
              type: "text",
              text:
                `**Memories for ${peerLabel}${keyLabel} (${entries.length} entries):**\n\n` +
                lines.join("\n"),
            } as const,
          ],
          details: { entries, count: entries.length } as any,
        };
      } catch (err: any) {
        return {
          content: [{ type: "text", text: `Failed to recall memories: ${err.message}` } as const],
          details: { entries: [] as any[], count: 0, error: err.message } as any,
        };
      }
    },
  });

  // ── memory_search ──────────────────────────────────────────────────────
  pi.registerTool({
    name: "memory_search",
    label: "Search Memory",
    description:
      "Semantic search across all stored memories. Finds entries by meaning, not just keywords. " +
      "Optionally scope to a specific peer. Results are filtered by a distance threshold " +
      "(default 0.6) — only meaningfully similar entries are returned.",
    promptSnippet: "Semantic search across all stored memories",
    promptGuidelines: [
      "Use memory_search to find relevant past conversations by meaning rather than by exact key. " +
        'Best for cross-cutting queries like "what decisions have we made about performance?"',
    ],
    parameters: Type.Object({
      query: Type.String({ description: "Natural language search query" }),
      peerId: Type.Optional(
        Type.String({ description: "Optional: scope to a specific peer" }),
      ),
      nResults: Type.Optional(
        Type.Number({ description: "Max results (default 5)" }),
      ),
      fullText: Type.Optional(
        Type.Boolean({
          description: "Return full untruncated values (default false)",
        }),
      ),
    }),

    async execute(_toolCallId: string, params: any, _signal?: AbortSignal) {
      if (!agentMemory) {
        return {
          content: [{ type: "text", text: "Memory is not available. ChromaDB may not be running." } as const],
          details: { results: [], count: 0, error: "memory not available" },
        };
      }

      try {
        const results = await agentMemory.search(params.query as string, {
          peerId: params.peerId as string | undefined,
          nResults: params.nResults as number | undefined,
          fullText: params.fullText as boolean | undefined,
        });

        if (results.length === 0) {
          // Check total entries for this peer to give helpful context
          const totalCount = params.peerId
            ? await agentMemory.count(params.peerId as string).catch(() => 0)
            : 0;
          const hint =
            totalCount > 0
              ? ` (${totalCount} total ${totalCount === 1 ? "entry exists" : "entries exist"} but none were semantically close enough — try a more specific query)`
              : "";
          return {
            content: [{ type: "text", text: `No memories found similar to "${params.query}".${hint}` } as const],
            details: { results: [], count: 0 },
          };
        }

        const lines = results.map((r, i) => {
          const age = Math.round((Date.now() - r.timestamp) / 60_000);
          const ageStr =
            age < 1 ? "just now" : age < 60 ? `${age}m ago` : `${Math.round(age / 60)}h ago`;
          const distIcon = r.distance < 0.3 ? "🟢" : r.distance < 0.5 ? "🟡" : "🟠";
          return `${i + 1}. ${distIcon} **${r.key}** (distance: ${r.distance.toFixed(2)}) — ${ageStr}\n   ${r.value}\n`;
        });

        return {
          content: [
            {
              type: "text",
              text:
                `**${results.length} memories similar to "${params.query}":**\n\n` +
                lines.join("\n"),
            } as const,
          ],
          details: { results, count: results.length },
        };
      } catch (err: any) {
        return {
          content: [{ type: "text", text: `Failed to search memories: ${err.message}` } as const],
          details: { results: [] as any[], count: 0, error: err.message } as any,
        };
      }
    },
  });

  // ── memory_keys ────────────────────────────────────────────────────────
  pi.registerTool({
    name: "memory_keys",
    label: "List Memory Keys",
    description:
      "List all keys stored for a specific peer, with entry counts. " +
      "Use this to discover what categories of information are available before recalling.",
    promptSnippet: "List memory keys for a peer",
    promptGuidelines: [
      "Use memory_keys to discover what categories of information you've stored about a peer before recalling specific entries.",
    ],
    parameters: Type.Object({
      peerId: Type.String({ description: "Peer to list keys for" }),
    }),

    async execute(_toolCallId: string, params: any, _signal?: AbortSignal) {
      if (!agentMemory) {
        return {
          content: [{ type: "text", text: "Memory is not available. ChromaDB may not be running." } as const],
          details: { keys: [] as any[], count: 0, totalEntries: 0, error: "memory not available" } as any,
        };
      }

      try {
        const keys = await agentMemory.getKeys(params.peerId as string);

        if (keys.length === 0) {
          return {
            content: [
              { type: "text", text: `No keys stored for peer ${(params.peerId as string).slice(0, 12)}….` } as const,
            ],
            details: { keys: [] as any[], count: 0, totalEntries: 0 } as any,
          };
        }

        const totalEntries = keys.reduce((sum, e) => sum + e.count, 0);
        const lines = keys.map(
          (e) => `  🔑 **${e.key}** (${e.count} ${e.count === 1 ? "entry" : "entries"})`,
        );

        return {
          content: [
            {
              type: "text",
              text:
                `**Keys for ${(params.peerId as string).slice(0, 12)}… (${totalEntries} total entries):**\n\n` +
                lines.join("\n"),
            } as const,
          ],
          details: { keys, count: keys.length, totalEntries } as any,
        };
      } catch (err: any) {
        return {
          content: [{ type: "text", text: `Failed to list keys: ${err.message}` } as const],
          details: { keys: [] as any[], count: 0, totalEntries: 0, error: err.message } as any,
        };
      }
    },
  });

  // ── memory_reconnect ───────────────────────────────────────────────────
  pi.registerTool({
    name: "memory_reconnect",
    label: "Reconnect Memory",
    description:
      "Manually reconnect to the ChromaDB collection. Use this after ChromaDB " +
      "restarts or if the collection was externally deleted and recreated. " +
      "Normally not needed — memory operations auto-recover on first failure.",
    promptSnippet: "Reconnect to ChromaDB memory collection",
    promptGuidelines: [
      "Use memory_reconnect after ChromaDB restarts or collection wipes if memory tools are returning errors.",
    ],
    parameters: Type.Object({}),

    async execute(_toolCallId: string, _params: any, _signal?: AbortSignal) {
      if (!agentMemory) {
        return {
          content: [{ type: "text", text: "Memory is not available. ChromaDB may not be running." } as const],
          details: { reconnected: false, error: "memory not available", collectionName: undefined as string | undefined },
        };
      }

      try {
        await agentMemory.reconnect();
        return {
          content: [
            {
              type: "text",
              text: `🔌 Reconnected to ChromaDB collection "${agentMemory.collectionName}".`,
            } as const,
          ],
          details: { reconnected: true, collectionName: agentMemory.collectionName, error: undefined as string | undefined },
        };
      } catch (err: any) {
        return {
          content: [{ type: "text", text: `Failed to reconnect: ${err.message}` } as const],
          details: { reconnected: false, error: err.message, collectionName: undefined as string | undefined },
        };
      }
    },
  });
}

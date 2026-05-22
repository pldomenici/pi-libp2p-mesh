/**
 * pi-libp2p-mesh — P2P mesh network for pi agents powered by libp2p.
 *
 * Architecture:
 *   - Each running pi agent spins up a libp2p node identified by a PeerId.
 *   - Peers auto-discover each other on the local network via mDNS and, optionally,
 *     the Kademlia DHT for wider-area discovery.
 *   - Direct agent-to-agent messaging uses a custom protocol (/pi-agent/0.1.0)
 *     built on libp2p streams with JSON request/response.
 *   - Broadcast messaging uses GossipSub on the topic "pi-broadcast".
 *   - Four custom tools expose the mesh to the LLM:
 *       mesh_list_peers   – list all known peers and their status
 *       mesh_send         – send a prompt/request to a specific peer
 *       mesh_broadcast    – broadcast a message to all connected peers
 *       mesh_discover     – trigger an active peer discovery scan
 */

import type { PeerId } from "@libp2p/interface";

// ── Peer ─────────────────────────────────────────────────────────────────────

export interface MeshPeer {
  /** Libp2p PeerId string (base58-encoded) */
  id: string;
  /** Multiaddrs this peer is reachable at */
  addresses: string[];
  /** Connection state */
  status: "connected" | "disconnected" | "connecting";
  /** Agent name (discovered via Identify protocol handshake) */
  agentName?: string;
  /** When this peer was first seen (epoch ms) */
  discoveredAt: number;
  /** When this peer last disconnected (epoch ms, 0 if still connected) */
  disconnectedAt?: number;
}

// ── Messages ─────────────────────────────────────────────────────────────────

export interface AgentRequest {
  /** Protocol version identifier */
  protocol: "/pi-agent/0.1.0";
  /** Unique request id (UUID v4) */
  requestId: string;
  /** Sender's agent name */
  fromAgent: string;
  /** Sender's PeerId */
  fromPeerId: string;
  /** Timestamp (epoch ms) */
  timestamp: number;
  /** The prompt / question / task */
  message: string;
  /** If true (default), auto-reply without involving the LLM. If false, forward to the LLM. */
  autoReply?: boolean;
  /** Optional per-request timeout override (ms). Default: 60_000. */
  timeoutMs?: number;
}

export interface AgentResponse {
  /** Echo of the request id */
  requestId: string;
  /** Responder's agent name */
  fromAgent: string;
  /** Responder's PeerId */
  fromPeerId: string;
  /** Timestamp (epoch ms) */
  timestamp: number;
  /** The response text */
  message: string;
  /** Whether the responder encountered an error */
  error: boolean;
}

/** GossipSub message envelope */
export interface BroadcastMessage {
  /** Source agent name */
  fromAgent: string;
  /** Source PeerId */
  fromPeerId: string;
  /** Timestamp (epoch ms) */
  timestamp: number;
  /** Message body */
  message: string;
  /** Optional message type hint */
  type?: "announce" | "query" | "response" | "event";
}

// ── Node Config ─────────────────────────────────────────────────────────────

export interface MeshConfig {
  /** Human-readable agent name for this node */
  agentName: string;
  /** Ports to listen on (TCP + WebSocket). Default: random. */
  listenPorts?: { tcp?: number; ws?: number };
  /** Enable mDNS local discovery (default: true) */
  enableMdns?: boolean;
  /** Enable Kademlia DHT for wide-area discovery (default: false) */
  enableDht?: boolean;
  /** Bootstrap multiaddrs for DHT */
  bootstrapPeers?: string[];
  /** GossipSub topic to use (default: "pi-broadcast") */
  gossipTopic?: string;
  /** Announce addresses (e.g., public IP for NAT traversal) */
  announceAddresses?: string[];
  /**
   * Optional pre-existing Ed25519 private key (raw bytes).
   * When provided, the PeerId is derived from this key instead of generating
   * a new one — giving the agent a stable identity across restarts.
   */
  privateKey?: Uint8Array;
}

/** Default configuration */
export const DEFAULT_CONFIG: Partial<MeshConfig> = {
  enableMdns: true,
  enableDht: false,
  gossipTopic: "pi-broadcast",
  listenPorts: { tcp: 0, ws: 0 },
};

// ── Node State ───────────────────────────────────────────────────────────────

export type MeshNodeEvent =
  | { type: "peer:discovered"; peer: MeshPeer }
  | { type: "peer:connected"; peerId: string }
  | { type: "peer:disconnected"; peerId: string }
  | { type: "peer:identified"; peerId: string; agentName: string; agentVersion: string }
  | { type: "message"; fromPeerId: string; request: AgentRequest }
  | { type: "broadcast"; message: BroadcastMessage }
  | { type: "error"; error: Error };

export type MeshNodeEventHandler = (event: MeshNodeEvent) => void;

// ── Tool Results ─────────────────────────────────────────────────────────────

export interface MeshSendResult {
  peerId: string;
  agentName?: string;
  response: AgentResponse | null;
  error?: string;
}

export interface MeshBroadcastResult {
  topic: string;
  peersReached: number;
  messageId: string;
}

export interface MeshDiscoverResult {
  peersFound: number;
  peers: MeshPeer[];
}

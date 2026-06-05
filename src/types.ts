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

// ── Peer ─────────────────────────────────────────────────────────────────────

export interface MeshPeer {
  /** Libp2p PeerId string (base58-encoded) */
  id: string;
  /** Multiaddrs this peer is reachable at */
  addresses: string[];
  /** Connection state */
  status: "connected" | "disconnected";
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
  /** If true, auto-reply without involving the LLM. If false or omitted (default), forward to the receiver's LLM. */
  autoReply?: boolean;
  /** Optional per-request timeout override (ms). Default: 60_000. */
  timeoutMs?: number;
  /** Extension version of the sender — for stale-build detection. */
  extensionVersion?: string;
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
  /**
   * Path to a swarm.key file for private network (PSK).
   * All peers must share the same key. When set, connections are wrapped in
   * an XSalsa20 stream cipher — peers without the key cannot communicate.
   */
  swarmKeyPath?: string;
  /** ChromaDB server hostname (default: localhost) */
  chromaHost?: string;
  /** ChromaDB server port (default: 8000) */
  chromaPort?: number;
  /** Optional auth token for ChromaDB (x-chroma-token header). Also CHROMA_TOKEN env var. */
  chromaToken?: string;
  /** Optional data directory for ChromaDB persistence (default: ~/.local/share/chroma). */
  chromaDataPath?: string;
}

/** Default configuration */
export const DEFAULT_CONFIG: Partial<MeshConfig> = {
  enableMdns: true,
  enableDht: false,
  gossipTopic: "pi-broadcast",
  listenPorts: { tcp: 0, ws: 0 },
  chromaHost: "localhost",
  chromaPort: 8000,
};

// ── Node State ───────────────────────────────────────────────────────────────

export type MeshNodeEvent =
  | { type: "peer:discovered"; peer: MeshPeer }
  | { type: "peer:connected"; peerId: string }
  | { type: "peer:disconnected"; peerId: string }
  | { type: "peer:identified"; peerId: string; agentName: string; agentVersion: string; extensionVersion?: string }
  | { type: "message"; fromPeerId: string; request: AgentRequest }
  | { type: "broadcast"; message: BroadcastMessage };

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

// ── Memory (ChromaDB) ───────────────────────────────────────────────────────

/** Configuration for memory read-side limits. Set via presets or individual flags. */
export interface MemoryConfig {
  /** Truncate entry values to this many chars before returning (default: 10,000) */
  valueTruncationChars: number;
  /** Hard max entries returned by get()/getByPeer() (default: 50) */
  maxEntries: number;
  /** Default limit when LLM omits it (default: 10) */
  defaultLimit: number;
  /** Most recent exchange truncated to this on auto-retrieve (default: 5,000) */
  exchangeTruncationChars: number;
  /** Max total chars injected by auto-retrieve hook (default: 50,000) */
  contextBudgetChars: number;
  /** Discard search results with distance above this (default: 0.6) */
  distanceThreshold: number;
  /** Default nResults for semantic search (default: 5) */
  searchNResults: number;
}

/** Preset memory configs for different context window sizes. */
export const MEMORY_PRESETS: Record<"small" | "medium" | "large", MemoryConfig> = {
  small: {
    valueTruncationChars: 2_000,
    maxEntries: 20,
    defaultLimit: 5,
    exchangeTruncationChars: 2_000,
    contextBudgetChars: 12_000,
    distanceThreshold: 0.6,
    searchNResults: 3,
  },
  medium: {
    valueTruncationChars: 5_000,
    maxEntries: 30,
    defaultLimit: 10,
    exchangeTruncationChars: 3_000,
    contextBudgetChars: 25_000,
    distanceThreshold: 0.6,
    searchNResults: 5,
  },
  large: {
    valueTruncationChars: 10_000,
    maxEntries: 50,
    defaultLimit: 10,
    exchangeTruncationChars: 5_000,
    contextBudgetChars: 50_000,
    distanceThreshold: 0.6,
    searchNResults: 5,
  },
};

/** A single memory entry stored in ChromaDB. */
export interface MemoryEntry {
  id: string;
  peerId: string;
  key: string;
  value: string;
  timestamp: number;
  type?: string;
  metadata?: Record<string, string | number>;
}

/** A memory entry returned by semantic search, including the cosine distance. */
export interface MemorySearchResult extends MemoryEntry {
  distance: number;
}

/** Key list entry returned by getKeys(). */
export interface MemoryKeyEntry {
  key: string;
  count: number;
}

// ── Memory Host Discovery (GossipSub) ─────────────────────────────────────

/**
 * Broadcast on the "pi-memory-host" GossipSub topic by the first node
 * that starts ChromaDB. All other nodes connect to this host.
 */
export interface MemoryHostAnnouncement {
  type: "memory:host";
  /** IP or hostname of the ChromaDB server. */
  host: string;
  /** Port the ChromaDB server is listening on. */
  port: number;
  /** Agent name of the host node. */
  fromAgent: string;
  /** PeerId of the host node. */
  fromPeerId: string;
  /** Timestamp of the announcement (epoch ms). */
  timestamp: number;
}

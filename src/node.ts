/**
 * pi-libp2p-mesh / node.ts
 *
 * libp2p node factory. Creates a configured libp2p instance with TCP + WebSocket
 * transports, Noise encryption, Yamux stream multiplexing, mDNS local discovery,
 * optional Kademlia DHT, Identify, and GossipSub pub/sub.
 */

import { createLibp2p, type Libp2p } from "libp2p";
import { tcp } from "@libp2p/tcp";
import { webSockets } from "@libp2p/websockets";
import { noise } from "@chainsafe/libp2p-noise";
import { yamux } from "@chainsafe/libp2p-yamux";
import { mdns } from "@libp2p/mdns";
import { identify } from "@libp2p/identify";
import { kadDHT, removePrivateAddressesMapper } from "@libp2p/kad-dht";
import { bootstrap } from "@libp2p/bootstrap";
import { gossipsub } from "@chainsafe/libp2p-gossipsub";
import { generateKeyPair, generateKeyPairFromSeed } from "@libp2p/crypto/keys";
import { peerIdFromString } from "@libp2p/peer-id";
import type { PeerId, PrivateKey, IdentifyResult } from "@libp2p/interface";

import {
  type MeshConfig,
  type MeshPeer,
  type MeshNodeEvent,
  type MeshNodeEventHandler,
  DEFAULT_CONFIG,
} from "./types.js";

// ── Constants ────────────────────────────────────────────────────────────────

/** Event bus topic for mesh events (shared across the extension). */
export const EVENT_BUS_TOPIC = "pi-libp2p-mesh";

// ── MeshNode ─────────────────────────────────────────────────────────────────

/**
 * A libp2p-based mesh node.
 *
 * Wraps a libp2p instance and emits structured events (MeshNodeEvent) that the
 * extension entry point (index.ts) forwards to pi's event bus / tool layer.
 *
 * @example
 * ```ts
 * const node = await MeshNode.create({ agentName: "pi-alpha" });
 * node.onEvent((ev) => console.log(ev.type));
 * await node.start();
 * console.log("PeerId:", node.peerId);
 * ```
 */
export class MeshNode {
  /** Base58-encoded PeerId of this node. */
  readonly peerId: string;
  /** The underlying libp2p instance. */
  readonly libp2p: Libp2p;
  /** Listening multiaddrs (populated after start()). */
  multiaddrs: string[] = [];
  /** Whether the node is currently running. */
  isRunning = false;

  private eventHandlers: MeshNodeEventHandler[] = [];
  declare private config: MeshConfig;

  // ── H2: Internal peer store ──────────────────────────────────────────
  // Persists peers across connect/disconnect cycles so disconnected peers
  // remain queryable (with status "disconnected" and a disconnectedAt
  // timestamp) rather than vanishing from getPeers().
  private peerStore = new Map<string, MeshPeer>();

  // ── H1: Debounced auto-dial ──────────────────────────────────────────
  // Collects newly discovered PeerIds and dials them in batches instead of
  // one-at-a-time, preventing connection storms under rapid mDNS discovery.
  private pendingDials = new Set<string>();
  private dialDebounceTimer: ReturnType<typeof setTimeout> | null = null;
  private static readonly DIAL_DEBOUNCE_MS = 200;

  private constructor(libp2p: Libp2p, config: MeshConfig) {
    this.libp2p = libp2p;
    this.peerId = libp2p.peerId.toString();
    this.config = config;
    this.peerStore = new Map();
    this.pendingDials = new Set();
  }

  // ── Factory ────────────────────────────────────────────────────────────────

  /**
   * Create a new MeshNode instance.
   *
   * Generates an Ed25519 keypair and PeerId, assembles the libp2p configuration,
   * and creates a libp2p node. Call `start()` afterwards to begin listening.
   */
  static async create(config: MeshConfig): Promise<MeshNode> {
    const mergedConfig = { ...DEFAULT_CONFIG, ...config };

    // M1: Accept a pre-existing private key for stable identity across restarts.
    // If provided, derive the PeerId from the seed; otherwise generate a fresh keypair.
    let privateKey: PrivateKey;

    if (mergedConfig.privateKey) {
      // Recreate the key from a persistent seed (raw Uint8Array)
      privateKey = await generateKeyPairFromSeed("Ed25519", mergedConfig.privateKey);
    } else {
      // Generate a new Ed25519 keypair (expensive — avoid on every restart)
      privateKey = await generateKeyPair("Ed25519");
    }

    // Assemble libp2p options
    // Use a local type alias instead of `any` for type-safety
    type Libp2pOptions = Parameters<typeof createLibp2p>[0];

    // Deep-merge listenPorts so that passing e.g. { listenPorts: { ws: 9000 } }
    // retains the default tcp: 0 instead of wiping it out.
    const listenTcp = config.listenPorts?.tcp ?? DEFAULT_CONFIG.listenPorts?.tcp ?? 0;
    const listenWs = config.listenPorts?.ws ?? DEFAULT_CONFIG.listenPorts?.ws ?? 0;

    const libp2pConfig: Libp2pOptions = {
      privateKey,
      addresses: {
        listen: [`/ip4/0.0.0.0/tcp/${listenTcp}`, `/ip4/0.0.0.0/tcp/${listenWs}/ws`],
        ...(mergedConfig.announceAddresses?.length
          ? { announce: mergedConfig.announceAddresses }
          : {}),
      },
      transports: [tcp(), webSockets()],
      connectionEncrypters: [noise()],
      streamMuxers: [yamux()],
      nodeInfo: {
        userAgent: `pi-libp2p-mesh/${mergedConfig.agentName}`,
      },
      services: {
        identify: identify(),
        pubsub: gossipsub({ allowPublishToZeroTopicPeers: true }) as any,
      },
    };

    // libp2pConfig is declared as Libp2pOptions — use any for dynamic mutations
    const cfg = libp2pConfig as any;

    // mDNS local discovery
    if (mergedConfig.enableMdns !== false) {
      cfg.peerDiscovery = cfg.peerDiscovery ?? [];
      cfg.peerDiscovery.push(mdns());
    }

    // Kademlia DHT for wider-area discovery
    if (mergedConfig.enableDht) {
      cfg.services.dht = kadDHT({
        protocol: "/ipfs/kad/1.0.0",
        peerInfoMapper: removePrivateAddressesMapper,
      });

      if (mergedConfig.bootstrapPeers?.length) {
        cfg.peerDiscovery = cfg.peerDiscovery ?? [];
        cfg.peerDiscovery.push(
          bootstrap({ list: mergedConfig.bootstrapPeers }),
        );
      }
    }

    const node = await createLibp2p(libp2pConfig);

    return new MeshNode(node, mergedConfig);
  }

  // ── Lifecycle ──────────────────────────────────────────────────────────────

  /**
   * Start the node: begin listening, start discovery services, and subscribe
   * to peer events.
   */
  async start(): Promise<void> {
    if (this.isRunning) return;

    // Wire internal event listeners before starting
    this.libp2p.addEventListener("peer:discovery", this._onPeerDiscovery);
    this.libp2p.addEventListener("peer:connect", this._onPeerConnect);
    this.libp2p.addEventListener("peer:disconnect", this._onPeerDisconnect);
    this.libp2p.addEventListener("peer:identify", this._onPeerIdentify);

    // Reset dial state for clean start
    this.pendingDials.clear();
    if (this.dialDebounceTimer) {
      clearTimeout(this.dialDebounceTimer);
      this.dialDebounceTimer = null;
    }

    await this.libp2p.start();
    this.isRunning = true;

    // Capture listening addresses
    this.multiaddrs = this.libp2p.getMultiaddrs().map((ma) => ma.toString());
  }

  /**
   * Stop the node gracefully: close all connections, stop services, and
   * remove event listeners.
   */
  async stop(): Promise<void> {
    if (!this.isRunning) return;

    this.libp2p.removeEventListener("peer:discovery", this._onPeerDiscovery);
    this.libp2p.removeEventListener("peer:connect", this._onPeerConnect);
    this.libp2p.removeEventListener("peer:disconnect", this._onPeerDisconnect);
    this.libp2p.removeEventListener("peer:identify", this._onPeerIdentify);

    // Cancel any pending dial debounce
    if (this.dialDebounceTimer) {
      clearTimeout(this.dialDebounceTimer);
      this.dialDebounceTimer = null;
    }
    this.pendingDials.clear();

    await this.libp2p.stop();
    this.isRunning = false;
  }

  // ── Peer Queries ───────────────────────────────────────────────────────────

  /**
   * Return all currently known peers (connected + disconnected) from the
   * internal peer store.  This is the single source of truth for peer
   * state — it does NOT query libp2p's transient connection list.
   *
   * Connected peers from libp2p that are not yet in the store are
   * backfilled automatically (handles inbound connections before mDNS).
   */
  getPeers(): MeshPeer[] {
    // Backfill any connected peers that libp2p knows about but our store
    // doesn't yet have (inbound connections before mDNS discovery)
    for (const peerId of this.libp2p.getPeers()) {
      const idStr = peerId.toString();
      if (!this.peerStore.has(idStr)) {
        this.peerStore.set(idStr, {
          id: idStr,
          addresses: this.libp2p
            .getConnections(peerId)
            .flatMap((c) => [c.remoteAddr.toString()])
            .filter(Boolean),
          status: "connected",
          discoveredAt: Date.now(),
        });
      }
    }

    return [...this.peerStore.values()];
  }

  /**
   * Remove peers that have been disconnected for longer than `ttlMs`.
   * Returns the number of stale entries removed.
   */
  pruneStalePeers(ttlMs: number = 60_000): number {
    const cutoff = Date.now() - ttlMs;
    let removed = 0;

    for (const [id, peer] of this.peerStore) {
      if (peer.status === "disconnected") {
        const lastSeen = peer.disconnectedAt ?? peer.discoveredAt;
        if (lastSeen < cutoff) {
          this.peerStore.delete(id);
          removed++;
        }
      }
    }

    return removed;
  }

  /** Clear ALL disconnected peers immediately. */
  pruneAllDisconnected(): number {
    let removed = 0;
    for (const [id, peer] of this.peerStore) {
      if (peer.status === "disconnected") {
        this.peerStore.delete(id);
        removed++;
      }
    }
    return removed;
  }

  // ── Event Subscriptions ────────────────────────────────────────────────────

  /**
   * Register a handler for MeshNodeEvent emissions.
   */
  onEvent(handler: MeshNodeEventHandler): void {
    this.eventHandlers.push(handler);
  }

  /** Emit an event to all registered handlers. */
  private emit(event: MeshNodeEvent): void {
    for (const handler of this.eventHandlers) {
      try {
        handler(event);
      } catch (err) {
        console.error("[MeshNode] Event handler error:", err);
      }
    }
  }

  // ── Internal libp2p Event Listeners ────────────────────────────────────────

  /**
   * Handle a peer:discovery event (mDNS, DHT, or bootstrap).
   *
   * Updates the peer store and triggers a DEBOUNCED auto-dial (H1 fix).
   * Instead of dialing immediately per-peer, newly discovered PeerIds are
   * collected and dialed in a single batch after a short coalescing window.
   * Duplicate discoveries for the same peer are ignored (dedup via pendingDials).
   */
  private _onPeerDiscovery = (evt: CustomEvent): void => {
    const detail = evt.detail;
    const peerId: PeerId = detail.id;
    const peerIdStr = peerId.toString();

    // Skip self (mDNS can discover our own addresses)
    if (peerIdStr === this.peerId) return;

    const addrs =
      detail.multiaddrs?.map((ma: any) => ma.toString()) ?? [];
    const existing = this.peerStore.get(peerIdStr);

    if (existing) {
      // Merge addresses into existing entry
      const addrSet = new Set(existing.addresses);
      for (const a of addrs) addrSet.add(a);
      existing.addresses = [...addrSet];
      existing.discoveredAt = Date.now();
    } else {
      this.peerStore.set(peerIdStr, {
        id: peerIdStr,
        addresses: addrs,
        status: "disconnected",
        discoveredAt: Date.now(),
      });
    }

    const stored = this.peerStore.get(peerIdStr)!;
    this.emit({ type: "peer:discovered", peer: stored });

    // ── H1: Debounced auto-dial ──────────────────────────────────────────
    // Collect in the pending set; dial the batch on a debounce timer.
    this.pendingDials.add(peerIdStr);

    if (!this.dialDebounceTimer) {
      this.dialDebounceTimer = setTimeout(() => {
        this.dialDebounceTimer = null;
        this._flushPendingDials();
      }, MeshNode.DIAL_DEBOUNCE_MS);
    }
  };

  /** Dial all peers collected in pendingDials with a single batch. */
  private _flushPendingDials(): void {
    const peers = [...this.pendingDials];
    this.pendingDials.clear();

    for (const peerIdStr of peers) {
      // Check that the peer isn't already connected (fast-path skip)
      const stored = this.peerStore.get(peerIdStr);
      if (stored?.status === "connected") continue;

      const peerIdObj = peerIdFromString(peerIdStr);
      this.libp2p
        .dial(peerIdObj)
        .catch((err: any) => {
          console.debug(
            `[MeshNode] auto-dial failed for ${peerIdStr.slice(0, 12)}…: ${err.message}`,
          );
        });
    }
  }

  private _onPeerConnect = (evt: CustomEvent): void => {
    const peerIdStr = evt.detail.toString();

    const stored = this.peerStore.get(peerIdStr);
    if (stored) {
      stored.status = "connected";
      // Clear the disconnected timestamp if this is a reconnection
      stored.disconnectedAt = undefined;
    } else {
      // Inbound connection before mDNS discovery — create placeholder
      this.peerStore.set(peerIdStr, {
        id: peerIdStr,
        addresses: [],
        status: "connected",
        discoveredAt: Date.now(),
      });
    }

    this.emit({ type: "peer:connected", peerId: peerIdStr });
  };

  private _onPeerDisconnect = (evt: CustomEvent): void => {
    const peerIdStr = evt.detail.toString();

    const stored = this.peerStore.get(peerIdStr);
    if (stored) {
      stored.status = "disconnected";
      stored.disconnectedAt = Date.now();
    }

    this.emit({ type: "peer:disconnected", peerId: peerIdStr });
  };

  private _onPeerIdentify = (evt: CustomEvent): void => {
    const detail = evt.detail as IdentifyResult;
    const peerIdStr = detail.peerId.toString();

    // Skip self-identify events
    if (peerIdStr === this.peerId) return;

    // Extract agent name from the agentVersion string.
    // Accepts both plain names (e.g. "bob") and prefixed ("pi-libp2p-mesh/bob").
    const agentVersion = detail.agentVersion ?? "";
    const prefix = "pi-libp2p-mesh/";
    const agentName = agentVersion.startsWith(prefix)
      ? agentVersion.slice(prefix.length)
      : agentVersion;

    const stored = this.peerStore.get(peerIdStr);
    if (stored) {
      stored.agentName = agentName;
    } else {
      // Identify before discovery/connect — create placeholder
      this.peerStore.set(peerIdStr, {
        id: peerIdStr,
        addresses: [],
        status: "connected",
        discoveredAt: Date.now(),
        agentName,
      });
    }

    this.emit({
      type: "peer:identified",
      peerId: peerIdStr,
      agentName,
      agentVersion,
    });
  };
}

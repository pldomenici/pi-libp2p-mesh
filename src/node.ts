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
import { identify, type IdentifyResult } from "@libp2p/identify";
import { kadDHT, removePrivateAddressesMapper } from "@libp2p/kad-dht";
import { bootstrap } from "@libp2p/bootstrap";
import { gossipsub } from "@chainsafe/libp2p-gossipsub";
import { generateKeyPair } from "@libp2p/crypto/keys";
import { peerIdFromPrivateKey } from "@libp2p/peer-id";
import { type Multiaddr } from "@multiformats/multiaddr";
import type { PeerId } from "@libp2p/interface";

import {
  type MeshConfig,
  type MeshPeer,
  type MeshNodeEvent,
  type MeshNodeEventHandler,
  DEFAULT_CONFIG,
} from "./types";

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
  private config: MeshConfig;

  private constructor(libp2p: Libp2p, config: MeshConfig) {
    this.libp2p = libp2p;
    this.peerId = libp2p.peerId.toString();
    this.config = config;
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

    // Generate keypair and PeerId
    const privateKey = await generateKeyPair("Ed25519");
    const peerId = peerIdFromPrivateKey(privateKey);

    // Assemble libp2p options
    const tcpPort = mergedConfig.listenPorts?.tcp ?? 0;
    const wsPort = mergedConfig.listenPorts?.ws ?? 0;

    const libp2pConfig: any = {
      privateKey,
      addresses: {
        listen: [`/ip4/0.0.0.0/tcp/${tcpPort}`, `/ip4/0.0.0.0/tcp/${wsPort}/ws`],
        ...(mergedConfig.announceAddresses?.length
          ? { announce: mergedConfig.announceAddresses }
          : {}),
      },
      transports: [tcp(), webSockets()],
      connectionEncrypters: [noise()],
      streamMuxers: [yamux()],
      services: {
        identify: identify({
          agentVersion: `pi-libp2p-mesh/${mergedConfig.agentName}`,
        }),
        pubsub: gossipsub(),
      },
    };

    // mDNS local discovery
    if (mergedConfig.enableMdns !== false) {
      libp2pConfig.peerDiscovery = libp2pConfig.peerDiscovery ?? [];
      libp2pConfig.peerDiscovery.push(mdns());
    }

    // Kademlia DHT for wider-area discovery
    if (mergedConfig.enableDht) {
      libp2pConfig.services.dht = kadDHT({
        protocol: "/ipfs/kad/1.0.0",
        peerInfoMapper: removePrivateAddressesMapper,
      });

      if (mergedConfig.bootstrapPeers?.length) {
        libp2pConfig.peerDiscovery = libp2pConfig.peerDiscovery ?? [];
        libp2pConfig.peerDiscovery.push(
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

    await this.libp2p.stop();
    this.isRunning = false;
  }

  // ── Peer Queries ───────────────────────────────────────────────────────────

  /**
   * Return all currently known peers and their connection status.
   */
  getPeers(): MeshPeer[] {
    const peers: MeshPeer[] = [];

    for (const peer of this.libp2p.getPeers()) {
      const connections = this.libp2p.getConnections(peer);
      const addresses = connections
        .flatMap((c) => [c.remoteAddr.toString()])
        .filter(Boolean);

      peers.push({
        id: peer.toString(),
        addresses: addresses.length > 0 ? addresses : ["unknown"],
        status: "connected",
        discoveredAt: Date.now(),
      });
    }

    return peers;
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

  private _onPeerDiscovery = (evt: CustomEvent): void => {
    const detail = evt.detail;
    const peerId: PeerId = detail.id;
    const peerIdStr = peerId.toString();

    const peer: MeshPeer = {
      id: peerIdStr,
      addresses: detail.multiaddrs?.map((ma: Multiaddr) => ma.toString()) ?? [],
      status: "disconnected",
      discoveredAt: Date.now(),
    };

    this.emit({ type: "peer:discovered", peer });

    // Auto-dial the discovered peer to establish a connection.
    // Skip self-dial (mDNS can discover our own addresses).
    if (peerIdStr === this.peerId) return;

    // (non-blocking — fire and forget with a small delay to let listeners register)
    setTimeout(() => {
      this.libp2p.dial(peerId).catch((err: any) => {
        console.debug(
          `[MeshNode] auto-dial failed for ${peerIdStr.slice(0, 12)}…: ${err.message}`,
        );
      });
    }, 500);
  };

  private _onPeerConnect = (evt: CustomEvent): void => {
    const peerId = evt.detail.toString();
    this.emit({ type: "peer:connected", peerId });
  };

  private _onPeerDisconnect = (evt: CustomEvent): void => {
    const peerId = evt.detail.toString();
    this.emit({ type: "peer:disconnected", peerId });
  };

  private _onPeerIdentify = (evt: CustomEvent): void => {
    const detail = evt.detail as IdentifyResult;
    const peerId = detail.peerId.toString();

    // Extract agent name from the agentVersion string.
    // Format: "pi-libp2p-mesh/<agentName>"
    const agentVersion = detail.agentVersion ?? "";
    let agentName = "";
    const prefix = "pi-libp2p-mesh/";
    if (agentVersion.startsWith(prefix)) {
      agentName = agentVersion.slice(prefix.length);
    }

    this.emit({
      type: "peer:identified",
      peerId,
      agentName,
      agentVersion,
    });
  };
}

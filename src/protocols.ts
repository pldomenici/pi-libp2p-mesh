/**
 * pi-libp2p-mesh — Protocol handler module.
 *
 * Provides the {@link MeshProtocols} class which manages:
 * - Direct agent-to-agent messaging over the `/pi-agent/0.1.0` protocol.
 * - Broadcast messaging via GossipSub.
 *
 * @packageDocumentation
 * @module protocols
 */

import { v4 as uuidv4 } from 'uuid';
import { peerIdFromString } from '@libp2p/peer-id';
import { encode, decode } from 'cborg';
import type { Libp2p, Stream } from '@libp2p/interface';
import type { GossipsubMessage } from '@chainsafe/libp2p-gossipsub';
import type { Uint8ArrayList } from 'uint8arraylist';
import type {
  AgentRequest,
  AgentResponse,
  BroadcastMessage,
  MeshConfig,
  MeshBroadcastResult,
  MeshPeer,
} from './types.js';

// ── Helpers ──────────────────────────────────────────────────────────────────

/**
/**
 * Read the entirety of a libp2p {@link Stream} into a single `Uint8Array`,
 * respecting an optional {@link AbortSignal} to prevent indefinite hangs.
 *
 * If the signal fires mid-read, the returned promise rejects with an
 * `AbortError` that the caller can distinguish from protocol-level errors.
 */
async function readStream(
  stream: Stream,
  signal?: AbortSignal,
): Promise<Uint8Array> {
  // Fast path: signal already aborted before we start
  if (signal?.aborted) {
    throw new DOMException("readStream aborted before start", "AbortError");
  }

  const chunks: Uint8Array[] = [];
  let totalBytes = 0;

  // Stream is AsyncIterable<Uint8Array | Uint8ArrayList> in v3
  for await (const raw of stream) {
    // Check for abort between chunks (prevents indefinite hang on
    // a stream that trickles data but never closes).
    if (signal?.aborted) {
      throw new DOMException("readStream aborted mid-read", "AbortError");
    }

    const chunk =
      raw instanceof Uint8Array
        ? raw
        : (raw as Uint8ArrayList).subarray();
    chunks.push(chunk);
    totalBytes += chunk.byteLength;
  }

  if (chunks.length === 0) return new Uint8Array(0);

  // Single-pass concatenation — only one copy of the data
  const result = new Uint8Array(totalBytes);
  let offset = 0;
  for (const chunk of chunks) {
    result.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return result;
}

// ── MeshProtocols ────────────────────────────────────────────────────────────

/**
 * Manages libp2p protocol handlers for the pi-agent mesh.
 *
 * **Direct messaging** (`/pi-agent/0.1.0`)
 * - Incoming requests are read as CBOR-encoded {@link AgentRequest}, an automatic echo
 *   {@link AgentResponse} is written back, and the registered `onMessage`
 *   callback is invoked.
 * - Outgoing requests use {@link MeshProtocols.sendMessage}.
 *
 * **Broadcast** (GossipSub)
 * - Subscribes to `config.gossipTopic` (default `"pi-broadcast"`).
 * - Incoming {@link BroadcastMessage}s are forwarded to the `onBroadcast`
 *   callback.
 * - Outgoing broadcasts use {@link MeshProtocols.broadcast}.
 *
 * @example
 * ```ts
 * const protocols = new MeshProtocols(libp2p, { agentName: "pi-gamma" });
 *
 * protocols.onMessage = (peerId, req) => {
 *   console.log(`Message from ${peerId}: ${req.message}`);
 * };
 *
 * protocols.onBroadcast = (msg) => {
 *   console.log(`Broadcast from ${msg.fromAgent}: ${msg.message}`);
 * };
 *
 * // Send a direct message
 * const resp = await protocols.sendMessage("12D3KooW...", {
 *   protocol: "/pi-agent/0.1.0",
 *   requestId: uuidv4(),
 *   fromAgent: "pi-gamma",
 *   message: "Hello!",
 * });
 *
 * // Broadcast a message
 * const result = await protocols.broadcast({
 *   fromAgent: "pi-gamma",
 *   message: "Hello everyone!",
 * });
 * ```
 */
export class MeshProtocols {
  private readonly libp2p: Libp2p;
  private readonly config: MeshConfig;
  private readonly protocol = '/pi-agent/0.1.0';

  private _onMessage?: (peerId: string, request: AgentRequest) => void;
  private _onBroadcast?: (msg: BroadcastMessage) => void;
  private _onRequest?: (peerId: string, request: AgentRequest) => Promise<string>;

  /**
   * @param libp2p - A started libp2p v3 node instance.
   * @param config - Mesh configuration (agent name, ports, topics, etc.).
   */
  constructor(libp2p: Libp2p, config: MeshConfig) {
    this.libp2p = libp2p;
    this.config = config;

    // 1. Register the direct-messaging protocol handler
    libp2p
      .handle(this.protocol, (stream, connection) => {
        this.handleIncomingMessage(
          stream,
          connection.remotePeer.toString(),
        ).catch((err) =>
          console.error('[mesh-protocols] handler error:', err),
        );
      })
      .catch((err: unknown) => {
        console.error('[mesh-protocols] failed to register handler:', err);
      });

    // 2. Subscribe to GossipSub for broadcast messages
    const topic = config.gossipTopic ?? 'pi-broadcast';
    const pubsub = this.resolvePubsub();
    if (pubsub != null) {
      pubsub.subscribe(topic);
      // NOTE: GossipSub emits 'gossipsub:message', not 'message'.
      // The detail is GossipsubMessage { msg, propagationSource, msgId },
      // not a bare libp2p Message.
      // Cast needed: GossipSub emits 'gossipsub:message' which is not in the
      // base PubSubEvents type — only GossipSub (not generic PubSub) has this.
      (pubsub as any).addEventListener('gossipsub:message', (event: CustomEvent<GossipsubMessage>) => {
        this.handleGossipMessage(event).catch((err: unknown) =>
          console.error('[mesh-protocols] gossip handler error:', err),
        );
      });
    } else {
      console.warn(
        '[mesh-protocols] GossipSub not available — broadcasts disabled',
      );
    }
  }

  // ── Callback setters ──────────────────────────────────────────────────────

  /**
   * Register a callback invoked when a direct message is received from a peer.
   *
   * @param cb - Handler receiving `(peerId: string, request: AgentRequest)`.
   */
  set onMessage(cb: (peerId: string, request: AgentRequest) => void) {
    this._onMessage = cb;
  }

  /**
   * Register a callback invoked when a broadcast message is received.
   *
   * @param cb - Handler receiving the parsed {@link BroadcastMessage}.
   */
  set onBroadcast(cb: (msg: BroadcastMessage) => void) {
    this._onBroadcast = cb;
  }

  /**
   * Register a callback invoked when a request with autoReply=false is received.
   * The callback receives the peer ID and request, and must return the response text.
   *
   * @param cb - Async handler returning the response string to send back.
   */
  set onRequest(cb: (peerId: string, request: AgentRequest) => Promise<string>) {
    this._onRequest = cb;
  }

  // ── Public API ────────────────────────────────────────────────────────────

  /**
   * Send a direct message to a specific peer and await their response.
   *
   * Automatically populates `fromPeerId` and `timestamp` on the outgoing
   * request.  A 30-second timeout is applied to the full round-trip.
   *
   * @param peerId - The target peer's base58-encoded PeerId string.
   * @param request - The request payload (all fields except `fromPeerId` and
   *   `timestamp`).
   * @returns The parsed {@link AgentResponse} from the remote peer.
   * @throws If the peer cannot be dialed, the stream times out, or the
   *   response is malformed.
   */
  async sendMessage(
    peerId: string,
    request: Omit<AgentRequest, 'fromPeerId' | 'timestamp'>,
  ): Promise<AgentResponse> {
    const peerIdObj = peerIdFromString(peerId);

    // Build the full request envelope
    const fullRequest: AgentRequest = {
      ...request,
      fromPeerId: this.libp2p.peerId.toString(),
      timestamp: Date.now(),
      autoReply: request.autoReply,
    };

    // Create an AbortController for the timeout (default 60s; per-request override)
    const timeoutMs = request.timeoutMs ?? 60_000;
    const abortController = new AbortController();
    const timeoutId = setTimeout(() => abortController.abort(), timeoutMs);

    let stream: Stream | null = null;
    try {
      // Dial the peer and open a protocol stream
      stream = await this.libp2p.dialProtocol(peerIdObj, [this.protocol], {
        signal: abortController.signal,
      });

      // Write the request to the stream (v3: send + close to signal end-of-request)
      stream.send(encode(fullRequest));
      await stream.close({ signal: abortController.signal });

      // Read the full response (abort-aware — prevents indefinite hang if the
      // remote peer closes write but never sends data)
      const raw = await readStream(stream, abortController.signal);
      return decode(raw) as AgentResponse;
    } finally {
      clearTimeout(timeoutId);
      if (stream != null) {
        try {
          await stream.close();
        } catch {
          // best-effort cleanup
        }
      }
    }
  }

  /**
   * Publish a broadcast message to all peers subscribed to the GossipSub
   * topic.
   *
   * Automatically populates `fromPeerId` and `timestamp`.
   *
   * @param message - The broadcast payload (without `fromPeerId` /
   *   `timestamp`).
   * @returns A {@link MeshBroadcastResult} containing the topic, estimated
   *   number of subscribers reached, and the message identifier.
   * @throws If GossipSub is not configured on the libp2p node.
   */
  async broadcast(
    message: Omit<BroadcastMessage, 'fromPeerId' | 'timestamp'>,
  ): Promise<MeshBroadcastResult> {
    const pubsub = this.resolvePubsub();
    if (pubsub == null) {
      throw new Error(
        'GossipSub is not available on this libp2p instance',
      );
    }

    const topic = this.config.gossipTopic ?? 'pi-broadcast';

    const fullMessage: BroadcastMessage = {
      ...message,
      fromPeerId: this.libp2p.peerId.toString(),
      timestamp: Date.now(),
    };

    const data = encode(fullMessage);
    const result = await pubsub.publish(topic, data);

    // Determine the number of subscribers reached.
    // GossipSub publish result may include recipients; fall back to topic subscriber count.
    const peersReached: number =
      (result as any).recipients?.length ?? pubsub.getSubscribers(topic).length;

    return {
      topic,
      peersReached,
      messageId: uuidv4(),
    };
  }

  /**
   * Placeholder callback invoked when a new peer is discovered on the network.
   *
   * May be overridden to trigger dialling, identity exchange, etc.
   *
   * @param peer - The discovered {@link MeshPeer}.
   */
  handlePeerDiscovered(peer: MeshPeer): void {
    console.debug(
      `[mesh-protocols] peer discovered: ${peer.id} (${
        peer.agentName ?? 'unnamed'
      })`,
    );
  }

  /**
   * Gracefully shut down all protocol handlers.
   *
   * - Unregisters the `/pi-agent/0.1.0` stream handler.
   * - Unsubscribes from the GossipSub broadcast topic.
   */
  async stop(): Promise<void> {
    // Unhandle the direct protocol
    try {
      await this.libp2p.unhandle(this.protocol);
    } catch {
      // Already removed — ignore.
    }

    // Unsubscribe from gossip topic
    const topic = this.config.gossipTopic ?? 'pi-broadcast';
    const pubsub = this.resolvePubsub();
    if (pubsub != null) {
      try {
        pubsub.unsubscribe(topic);
      } catch {
        // Already unsubscribed — ignore.
      }
    }
  }

  // ── Private ───────────────────────────────────────────────────────────────

  /**
   * Handle an incoming direct-messaging stream.
   *
   * Reads the `AgentRequest` from the stream, writes an automatic echo
   * `AgentResponse`, and notifies the `onMessage` callback.
   */
  private async handleIncomingMessage(
    stream: Stream,
    peerIdStr: string,
  ): Promise<void> {
    try {
      // Read the full request
      const raw = await readStream(stream);
      const request: AgentRequest = decode(raw) as AgentRequest;

      let responseMessage: string;

      if (request.autoReply === true) {
        // Explicit auto-reply: echo without involving the LLM
        responseMessage = `[auto-response] Received: "${request.message}"`;
      } else if (this._onRequest) {
        // Forward to LLM (default behavior when autoReply is not true)
        responseMessage = await this._onRequest(peerIdStr, request);
      } else {
        // Fallback: no LLM handler registered
        responseMessage = `[auto-response] Received (no LLM handler): "${request.message}"`;
      }

      const response: AgentResponse = {
        requestId: request.requestId,
        fromAgent: this.config.agentName,
        fromPeerId: this.libp2p.peerId.toString(),
        timestamp: Date.now(),
        message: responseMessage,
        error: false,
      };

      // Write the response back (v3: send); the finally block handles close.
      stream.send(encode(response));

      // Notify the registered callback (for logging/side effects)
      this._onMessage?.(peerIdStr, request);
    } catch (err) {
      // M2: Write an error response so the sender fails fast (<100ms)
      // instead of waiting for its full 30s timeout.
      console.error('[mesh-protocols] error handling incoming message:', err);

      const errorResponse: AgentResponse = {
        requestId: "unknown",
        fromAgent: this.config.agentName,
        fromPeerId: this.libp2p.peerId.toString(),
        timestamp: Date.now(),
        message: `Error processing request: ${err instanceof Error ? err.message : "unknown error"}`,
        error: true,
      };

      try {
        stream.send(encode(errorResponse));
        await stream.close();
      } catch {
        // Best-effort — stream may already be broken
      }
    } finally {
      try {
        await stream.close();
      } catch {
        // Best-effort cleanup
      }
    }
  }

  /**
   * Handle an incoming GossipSub message event.
   *
   * Parses the raw bytes as a `BroadcastMessage` and invokes the
   * `onBroadcast` callback.
   */
  private async handleGossipMessage(
    event: CustomEvent<GossipsubMessage>,
  ): Promise<void> {
    const { msg: message } = event.detail;
    const broadcastMsg: BroadcastMessage = decode(message.data) as BroadcastMessage;

    this._onBroadcast?.(broadcastMsg);
  }

  /**
  /**
   * Resolve the GossipSub pubsub instance from wherever it is mounted
   * on the libp2p node.
   *
   * Checks `libp2p.services.pubsub` first (libp2p v3 service pattern), then
   * falls back to `libp2p.pubsub` for compatibility.
   *
   * @returns The pubsub service instance, or `null` if not available.
   */
  private resolvePubsub(): any | null {
    const libp2pAny = this.libp2p as unknown as Record<string, unknown>;
    const svc = libp2pAny.services as Record<string, unknown> | undefined;
    if (svc?.pubsub != null) return svc.pubsub as any;
    if (libp2pAny.pubsub != null) {
      return libp2pAny.pubsub as any;
    }
    return null;
  }
}

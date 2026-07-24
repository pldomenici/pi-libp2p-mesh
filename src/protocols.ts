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
import type { GossipsubMessage } from '@libp2p/gossipsub';
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

  // Build a promise that rejects when the signal fires.
  // We race this against the stream iteration so we don't block
  // forever when a remote peer opens a stream but never sends data.
  const abortPromise = new Promise<never>((_, reject) => {
    if (signal?.aborted) {
      reject(new DOMException("readStream aborted", "AbortError"));
      return;
    }
    const onAbort = () => {
      reject(new DOMException("readStream aborted", "AbortError"));
    };
    signal?.addEventListener('abort', onAbort, { once: true });
  });

  const readPromise = (async (): Promise<Uint8Array> => {
    const chunks: Uint8Array[] = [];
    let totalBytes = 0;

    // Stream is AsyncIterable<Uint8Array | Uint8ArrayList> in v3
    for await (const raw of stream) {
      const chunk =
        raw instanceof Uint8Array
          ? raw
          : (raw as unknown as Uint8ArrayList).subarray();
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
  })();

  return Promise.race([readPromise, abortPromise]);
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
  private readonly extensionVersion?: string;

  private _onMessage?: (peerId: string, request: AgentRequest) => void;
  private _onBroadcast?: (msg: BroadcastMessage) => void;
  private _onRequest?: (peerId: string, request: AgentRequest) => Promise<string>;

  /**
   * @param libp2p - A started libp2p v3 node instance.
   * @param config - Mesh configuration (agent name, ports, topics, etc.).
   */
  constructor(libp2p: Libp2p, config: MeshConfig, extensionVersion?: string) {
    this.libp2p = libp2p;
    this.config = config;
    this.extensionVersion = extensionVersion;

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
        // GossipSub-specific event (not in base PubSubEvents type)
      ;(pubsub as { addEventListener: (evt: string, cb: (event: CustomEvent<GossipsubMessage>) => void) => void }).addEventListener('gossipsub:message', (event: CustomEvent<GossipsubMessage>) => {
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
      extensionVersion: this.extensionVersion,
    };

    // Create an AbortController for the timeout (default 60s; per-request override)
    const timeoutMs = request.timeoutMs ?? 15_000;

    // Retry loop: handle streams that arrive closed due to connection races
    const MAX_RETRIES = 2;
    let lastError: Error | undefined;

    for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
      const abortController = new AbortController();
      const timeoutId = setTimeout(() => abortController.abort(), timeoutMs);

      let stream: Stream | null = null;

      const abortPromise = new Promise<never>((_, reject) => {
        if (abortController.signal.aborted) {
          reject(new DOMException("Dial aborted (timeout)", "AbortError"));
          return;
        }
        abortController.signal.addEventListener("abort", () => {
          reject(new DOMException("Dial aborted (timeout)", "AbortError"));
        }, { once: true });
      });

      try {
        // Dial the peer and open a protocol stream
        stream = await Promise.race([
          this.libp2p.dialProtocol(peerIdObj, [this.protocol], {
            signal: abortController.signal,
          }),
          abortPromise,
        ]);

        // Guard: stream may arrive already closed due to yamux connection races
        if (stream.writeStatus !== 'writable') {
          throw new DOMException(
            `Stream not writable (status: ${stream.writeStatus})`,
            "StreamClosed"
          );
        }

        // Write the request with backpressure handling
        const encoded = encode(fullRequest);
        if (!stream.send(encoded)) {
          // Write buffer full — wait for drain before closing
          await new Promise<void>((resolve, reject) => {
            const onDrain = () => { cleanup(); resolve(); };
            const onClose = (evt: any) => {
              cleanup();
              reject(new Error(evt?.detail?.error?.message || 'Stream closed while waiting for drain'));
            };
            const cleanup = () => {
              stream!.removeEventListener('drain', onDrain);
              stream!.removeEventListener('close', onClose);
            };
            stream!.addEventListener('drain', onDrain, { once: true });
            stream!.addEventListener('close', onClose, { once: true });
          });
        }
        await stream.close({ signal: abortController.signal });

        // Read the full response
        const raw = await readStream(stream, abortController.signal);
        return decode(raw) as AgentResponse;
      } catch (err: any) {
        lastError = err;
        // Only retry on transient stream errors (race conditions, resets)
        if (err.name === 'StreamClosed' || err.message?.includes('not writable') || err.message?.includes('reset')) {
          if (attempt < MAX_RETRIES) {
            console.debug(
              `[mesh-protocols] retrying sendMessage to ${peerId.slice(0, 12)}… (attempt ${attempt + 1}/${MAX_RETRIES + 1})`,
            );
            continue;
          }
        }
        throw err;
      } finally {
        clearTimeout(timeoutId);
        if (stream != null) {
          try { await stream.close(); } catch { /* best-effort */ }
        }
      }
    }

    throw lastError ?? new Error('sendMessage failed after retries');
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
   * Subscribe to an arbitrary GossipSub topic with a raw CBOR-decode callback.
   * Used for non-agent messages like memory host announcements.
   */
  subscribeRawTopic<T>(topic: string, onMessage: (msg: T, fromPeerId: string) => void): void {
    const pubsub = this.resolvePubsub();
    if (pubsub == null) {
      console.warn(`[mesh-protocols] GossipSub not available — cannot subscribe to "${topic}"`);
      return;
    }
    pubsub.subscribe(topic);
    ;(pubsub as { addEventListener: (evt: string, cb: (event: CustomEvent<GossipsubMessage>) => void) => void }).addEventListener('gossipsub:message', (event: CustomEvent<GossipsubMessage>) => {
      const gMsg = event.detail;
      // We only handle messages for our specific topic
      if (gMsg.msg.topic !== topic) return;
      try {
        const decoded = decode(gMsg.msg.data) as T;
        // Skip our own messages (GossipSub delivers to publisher too)
        if ((decoded as any).fromPeerId === this.libp2p.peerId.toString()) return;
        // from may be absent on unsigned messages — cast is safe for gossipsub
        const fromPeerId = (gMsg.msg as { from?: { toString(): string } }).from?.toString?.() ?? "";
        onMessage(decoded, fromPeerId);
      } catch (err) {
        console.warn(`[mesh-protocols] Failed to decode message on "${topic}":`, err);
      }
    });
  }

  /**
   * Publish an arbitrary CBOR-encoded message on a GossipSub topic.
   */
  async publishRawTopic<T>(topic: string, message: T): Promise<void> {
    const pubsub = this.resolvePubsub();
    if (pubsub == null) {
      throw new Error("GossipSub not available");
    }
    await pubsub.publish(topic, encode(message as any));
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
  private static readonly INCOMING_STREAM_TIMEOUT_MS = 15_000;

  private async handleIncomingMessage(
    stream: Stream,
    peerIdStr: string,
  ): Promise<void> {
    try {
      // Read the full request with a timeout to prevent indefinite hang
      // from a misbehaving peer that opens a stream but never half-closes.
      const abortController = new AbortController();
      const timeoutId = setTimeout(() => abortController.abort(), MeshProtocols.INCOMING_STREAM_TIMEOUT_MS);
      let raw: Uint8Array;
      try {
        raw = await readStream(stream, abortController.signal);
      } finally {
        clearTimeout(timeoutId);
      }
      const request: AgentRequest = decode(raw) as AgentRequest;

      // Notify onMessage callback (logging / side effects)
      Promise.resolve(this._onMessage?.(peerIdStr, request)).catch((err) => {
        console.error('[mesh-protocols] onMessage handler error:', err);
      });

      if (request.autoReply === true) {
        // Explicit auto-reply: echo without involving the LLM
        const response: AgentResponse = {
          requestId: request.requestId,
          fromAgent: this.config.agentName,
          fromPeerId: this.libp2p.peerId.toString(),
          timestamp: Date.now(),
          message: `[auto-response] Received: "${request.message}"`,
          error: false,
        };
        stream.send(encode(response));
      } else if (this._onRequest) {
        // ── Backward-compatible LLM processing ────────────────────────
        // If the sender lacks an extensionVersion, it is running old code
        // that expects a synchronous response on this stream. Await the
        // LLM and respond inline so the sender doesn't time out.
        if (!request.extensionVersion) {
          // Old sender — sync LLM (backward compat)
          const responseMessage = await this._onRequest(peerIdStr, request);
          const response: AgentResponse = {
            requestId: request.requestId,
            fromAgent: this.config.agentName,
            fromPeerId: this.libp2p.peerId.toString(),
            timestamp: Date.now(),
            message: responseMessage,
            error: false,
          };
          stream.send(encode(response));
        } else {
          // ── Async LLM processing ───────────────────────────────────────
          // Write ACK immediately so the sender doesn't block on our LLM
          // (which may take 10-60s). The real response arrives later via a
          // follow-up mesh_send with responseToRequestId set.
          const ack: AgentResponse = {
            requestId: request.requestId,
            fromAgent: this.config.agentName,
            fromPeerId: this.libp2p.peerId.toString(),
            timestamp: Date.now(),
            message: `[queued] Accepted — response will arrive asynchronously`,
            error: false,
            queued: true,
          };
          stream.send(encode(ack));
          await stream.close();

          // Fire-and-forget: process through LLM. The callback is responsible
          // for sending the response back via mesh_send.
          this._onRequest(peerIdStr, request).catch((err) =>
            console.error('[mesh-protocols] async onRequest error:', err),
          );

          // Skip the normal response+close path below (stream already closed)
          return;
        }
      } else {
        // Fallback: no LLM handler registered
        const response: AgentResponse = {
          requestId: request.requestId,
          fromAgent: this.config.agentName,
          fromPeerId: this.libp2p.peerId.toString(),
          timestamp: Date.now(),
          message: `[auto-response] Received (no LLM handler): "${request.message}"`,
          error: false,
        };
        stream.send(encode(response));
      }
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

    // Only handle the main broadcast topic — other topics (e.g. memory host
    // announcements) are handled by their own subscribeRawTopic listeners.
    const topic = this.config.gossipTopic ?? 'pi-broadcast';
    if (message.topic !== topic) return;

    const broadcastMsg: BroadcastMessage = decode(message.data) as BroadcastMessage;
    // Wrap in Promise.resolve to catch any async rejections
    Promise.resolve(this._onBroadcast?.(broadcastMsg)).catch((err) => {
      console.error('[mesh-protocols] onBroadcast handler error:', err);
    });
  }

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

/**
 * pi-libp2p-mesh / mission-control — HTTP + WebSocket bridge server.
 *
 * Serves the mission-control frontend and streams live P2P mesh state
 * to connected browsers via WebSocket.
 *
 * Usage (programmatic):
 *   import { MissionControlServer } from './mission-control/server.js';
 *   const mcs = new MissionControlServer({ port: 9191 });
 *   await mcs.start();
 *   mcs.updateState({ peers: [...], self: {...} });
 *   mcs.emitMessage({ from: 'a', to: 'b', ... });
 *   await mcs.stop();
 */

import { createServer, type IncomingMessage, type ServerResponse, type Server } from "node:http";
import { readFile } from "node:fs/promises";
import { join, extname } from "node:path";
import { fileURLToPath } from "node:url";
import { WebSocketServer, type WebSocket } from "ws";

// ── Types ────────────────────────────────────────────────────────────────────

export interface MeshPeerState {
  peerId: string;
  agentName?: string;
  status: "connected" | "disconnected";
  addresses: string[];
  discoveredAt: number;
  disconnectedAt?: number;
  /** Messages sent TO this peer */
  messagesTo: number;
  /** Messages received FROM this peer */
  messagesFrom: number;
  /** Assigned role (if known) */
  role?: string;
}

export interface SelfState {
  peerId: string;
  agentName: string;
  addresses: string[];
}

export interface CommLink {
  from: string;
  to: string;
  count: number;
  lastTimestamp: number;
  /** Average round-trip time in milliseconds for this link. */
  avgRttMs: number;
}

export interface MeshState {
  self: SelfState;
  peers: MeshPeerState[];
  stats: MeshStats;
  /** Communication graph edges */
  commLinks: CommLink[];
}

export interface MeshStats {
  totalPeers: number;
  connectedPeers: number;
  messagesSent: number;
  messagesReceived: number;
  broadcastsSent: number;
  broadcastsReceived: number;
  /** Per-second message rate (5s window) */
  messagesPerSec: number;
  /** Number of pending LLM requests awaiting agent_end resolution. */
  pendingQueueDepth: number;
  /** Cumulative error count. */
  errors: number;
}

export interface MessageFlow {
  from: string;
  fromName: string;
  to: string;
  toName: string;
  direction: "sent" | "received";
  sizeBytes: number;
  timestamp: number;
}

export interface BroadcastFlow {
  from: string;
  fromName: string;
  message: string;
  type?: string;
  timestamp: number;
}

export type OutgoingWSMessage =
  | { type: "state"; data: MeshState }
  | { type: "message"; data: MessageFlow }
  | { type: "broadcast"; data: BroadcastFlow };

// ── MIME ─────────────────────────────────────────────────────────────────────

const MIME: Record<string, string> = {
  ".html": "text/html; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".js": "application/javascript; charset=utf-8",
  ".mjs": "application/javascript; charset=utf-8",
  ".json": "application/json",
  ".png": "image/png",
  ".svg": "image/svg+xml",
  ".ico": "image/x-icon",
};

// ── Server ───────────────────────────────────────────────────────────────────

export class MissionControlServer {
  readonly port: number;
  private httpServer: Server | null = null;
  private wss: WebSocketServer | null = null;
  private clients: Set<WebSocket> = new Set();
  private lastState: MeshState | null = null;

  /** Resolved absolute path to the public/ directory (relative to dist/). */
  private get publicDir(): string {
    const thisDir = fileURLToPath(import.meta.url);
    // dist/mission-control-server.js → project-root/mission-control/public/
    return join(thisDir, "..", "..", "mission-control", "public");
  }

  constructor(opts: { port?: number } = {}) {
    this.port = opts.port ?? 9191;
  }

  // ── Lifecycle ────────────────────────────────────────────────────────────

  async start(): Promise<void> {
    if (this.httpServer) return; // already running

    this.httpServer = createServer((req, res) => this._handleHttp(req, res));

    this.wss = new WebSocketServer({ server: this.httpServer, path: "/ws" });
    this.wss.on("connection", (ws) => {
      this.clients.add(ws);
      console.log(
        `[mission-control] browser connected (${this.clients.size} total)`,
      );

      // Send current state immediately so the new client doesn't start blank
      if (this.lastState) {
        ws.send(JSON.stringify({ type: "state", data: this.lastState }));
      }

      ws.on("close", () => {
        this.clients.delete(ws);
        console.log(
          `[mission-control] browser disconnected (${this.clients.size} remaining)`,
        );
      });

      ws.on("error", () => {
        this.clients.delete(ws);
      });
    });

    return new Promise((resolve, reject) => {
      this.httpServer!.listen(this.port, () => {
        console.log(
          `[mission-control] server listening on http://localhost:${this.port}`,
        );
        resolve();
      });
      this.httpServer!.on("error", reject);
    });
  }

  async stop(): Promise<void> {
    // Close all WebSocket connections
    for (const ws of this.clients) {
      ws.close();
    }
    this.clients.clear();

    if (this.wss) {
      this.wss.close();
      this.wss = null;
    }

    if (this.httpServer) {
      return new Promise((resolve) => {
        this.httpServer!.close(() => {
          console.log("[mission-control] server stopped");
          this.httpServer = null;
          resolve();
        });
      });
    }
  }

  // ── State Push API ────────────────────────────────────────────────────────

  /** Push a full mesh state snapshot to all connected browsers. */
  updateState(state: MeshState): void {
    this.lastState = state;
    this._broadcast({ type: "state", data: state });
  }

  /** Emit a direct-message flow event (animated particle between nodes). */
  emitMessage(flow: MessageFlow): void {
    this._broadcast({ type: "message", data: flow });
  }

  /** Emit a broadcast flow event. */
  emitBroadcast(flow: BroadcastFlow): void {
    this._broadcast({ type: "broadcast", data: flow });
  }

  // ── Private ───────────────────────────────────────────────────────────────

  private _broadcast(msg: OutgoingWSMessage): void {
    const raw = JSON.stringify(msg);
    for (const ws of this.clients) {
      if (ws.readyState === ws.OPEN) {
        ws.send(raw);
      }
    }
  }

  /** Minimal static file server for the public/ directory. */
  private async _handleHttp(
    req: IncomingMessage,
    res: ServerResponse,
  ): Promise<void> {
    const url = new URL(req.url ?? "/", `http://localhost:${this.port}`);
    let pathname = url.pathname;

    // Default → index.html
    if (pathname === "/" || pathname.endsWith("/")) {
      pathname = "/index.html";
    }

    // Security: prevent directory traversal
    if (pathname.includes("..")) {
      res.writeHead(403);
      res.end("Forbidden");
      return;
    }

    const filePath = join(this.publicDir, pathname);
    const ext = extname(filePath).toLowerCase();
    const contentType = MIME[ext] ?? "application/octet-stream";

    try {
      const data = await readFile(filePath);
      res.writeHead(200, { "Content-Type": contentType });
      res.end(data);
    } catch {
      res.writeHead(404);
      res.end("Not found");
    }
  }
}

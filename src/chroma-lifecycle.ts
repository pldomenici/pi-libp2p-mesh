/**
 * pi-libp2p-mesh / chroma-lifecycle.ts
 *
 * Manages the ChromaDB server lifecycle:
 *   - Health checks (is ChromaDB reachable?)
 *   - Auto-start when not running (spawn as child process)
 *   - Graceful stop on session shutdown
 *   - Binary discovery (find `chroma` in PATH + common locations)
 */

import { spawn, type ChildProcess } from "node:child_process";
import * as path from "node:path";
import * as os from "node:os";

// ── Types ────────────────────────────────────────────────────────────────────

export interface ChromaDBLifecycleConfig {
  host: string;
  port: number;
  token?: string;
  /** Optional data directory for ChromaDB persistence. Default: ~/.local/share/chroma */
  dataPath?: string;
}

// ── ChromaDBLifecycle ────────────────────────────────────────────────────────

export class ChromaDBLifecycle {
  readonly config: ChromaDBLifecycleConfig;

  private childProcess: ChildProcess | null = null;
  private startedByUs: boolean = false;

  constructor(config: ChromaDBLifecycleConfig) {
    this.config = config;
  }

  // ── Health Check ────────────────────────────────────────────────────────────

  /**
   * Check if ChromaDB is reachable at the configured host:port.
   * Uses a short timeout (2s) so the check is fast on the happy path.
   */
  async isRunning(): Promise<boolean> {
    try {
      const url = `http://${this.config.host}:${this.config.port}/api/v2/heartbeat`;
      const headers: Record<string, string> = {};
      if (this.config.token) {
        headers["x-chroma-token"] = this.config.token;
      }
      const resp = await fetch(url, {
        headers,
        signal: AbortSignal.timeout(2000),
      });
      return resp.ok;
    } catch {
      return false;
    }
  }

  // ── Auto-Start ──────────────────────────────────────────────────────────────

  /**
   * Ensure ChromaDB is running.
   *
   * - If already reachable → returns immediately.
   * - If not reachable AND host is localhost → attempts to start it.
   * - If host is not localhost → returns false (we don't remote-start).
   *
   * Returns true if ChromaDB is confirmed reachable after this call.
   */
  async ensureRunning(): Promise<boolean> {
    if (await this.isRunning()) {
      console.log(
        `[pi-libp2p-mesh] ChromaDB already running at ${this.config.host}:${this.config.port}`,
      );
      return true;
    }

    // Only auto-start on localhost
    if (this.config.host !== "localhost" && this.config.host !== "127.0.0.1") {
      console.warn(
        `[pi-libp2p-mesh] ChromaDB unreachable at ${this.config.host}:${this.config.port} — not auto-starting (remote host)`,
      );
      return false;
    }

    console.log(
      `[pi-libp2p-mesh] ChromaDB not running — attempting auto-start on port ${this.config.port}…`,
    );

    // Find the chroma binary
    const chromaBin = await this._findChromaBinary();
    if (!chromaBin) {
      console.warn(
        "[pi-libp2p-mesh] chroma binary not found. Install it with: pip install chromadb",
      );
      return false;
    }

    // Resolve data path
    const dataPath =
      this.config.dataPath ??
      path.resolve(os.homedir(), ".local", "share", "chroma");

    // Start ChromaDB as a child process
    try {
      const proc = spawn(
        chromaBin,
        ["run", "--host", "0.0.0.0", "--path", dataPath, "--port", String(this.config.port)],
        {
          stdio: ["ignore", "pipe", "pipe"],
        },
      );

      // Log ChromaDB's own output for debugging
      proc.stdout?.on("data", (chunk: Buffer) => {
        const lines = chunk.toString().trim();
        if (lines) {
          for (const line of lines.split("\n")) {
            console.log(`[chromadb] ${line}`);
          }
        }
      });
      proc.stderr?.on("data", (chunk: Buffer) => {
        const lines = chunk.toString().trim();
        if (lines) {
          for (const line of lines.split("\n")) {
            // chroma prints some non-error stuff to stderr; only warn on actual errors
            if (line.toLowerCase().includes("error") || line.toLowerCase().includes("panic")) {
              console.warn(`[chromadb:err] ${line}`);
            }
          }
        }
      });

      this.childProcess = proc;
      this.startedByUs = true;

      // Wait for ChromaDB to be ready (poll up to 15s)
      const ready = await this._waitUntilReady(15_000);
      if (ready) {
        console.log(
          `[pi-libp2p-mesh] ChromaDB started — data at ${dataPath}, listening on ${this.config.host}:${this.config.port}`,
        );
        return true;
      }

      // Started but never responded — kill it
      console.warn("[pi-libp2p-mesh] ChromaDB failed to become ready in time — stopping");
      this.stop();
      return false;
    } catch (err: any) {
      console.warn(
        `[pi-libp2p-mesh] Failed to start ChromaDB: ${err.message}`,
      );
      return false;
    }
  }

  // ── Stop ───────────────────────────────────────────────────────────────────

  /**
   * Stop the ChromaDB child process if we started it.
   * Sends SIGTERM and gives it 5s to exit gracefully before SIGKILL.
   */
  stop(): void {
    if (!this.childProcess || !this.startedByUs) return;

    const proc = this.childProcess;
    this.childProcess = null;
    this.startedByUs = false;

    console.log("[pi-libp2p-mesh] Stopping ChromaDB…");

    // Give it a chance to exit gracefully
    const forceKill = setTimeout(() => {
      try { proc.kill("SIGKILL"); } catch { /* already exited */ }
    }, 5000);

    proc.on("exit", () => {
      clearTimeout(forceKill);
      console.log("[pi-libp2p-mesh] ChromaDB stopped");
    });

    try {
      proc.kill("SIGTERM");
    } catch {
      // Already exited
      clearTimeout(forceKill);
    }
  }

  // ── Private Helpers ────────────────────────────────────────────────────────

  /** Poll /api/v2/heartbeat until ChromaDB responds or timeout elapses. */
  private async _waitUntilReady(timeoutMs: number): Promise<boolean> {
    const deadline = Date.now() + timeoutMs;
    const pollInterval = 300;

    // Give it an initial grace period before polling
    await new Promise((r) => setTimeout(r, 800));

    while (Date.now() < deadline) {
      // If the child process died, give up immediately
      if (this.childProcess?.exitCode !== null && this.childProcess?.exitCode !== undefined) {
        return false;
      }

      if (await this.isRunning()) return true;
      await new Promise((r) => setTimeout(r, pollInterval));
    }

    return false;
  }

  /** Locate the `chroma` binary on this system. */
  private async _findChromaBinary(): Promise<string | null> {
    // Standard candidates in priority order
    const candidates = [
      "chroma", // in PATH
      path.resolve(os.homedir(), ".local", "bin", "chroma"), // pip --user on Linux
      path.resolve(os.homedir(), "Library", "Python", "3", "bin", "chroma"), // macOS homebrew python
      path.resolve(os.homedir(), "AppData", "Roaming", "Python", "Scripts", "chroma.exe"), // Windows pip --user
      "/usr/local/bin/chroma",
    ];

    for (const candidate of candidates) {
      try {
        await this._testBin(candidate);
        return candidate;
      } catch {
        continue;
      }
    }

    return null;
  }

  /** Test if a binary runs (in --version mode). */
  private async _testBin(binPath: string): Promise<void> {
    return new Promise((resolve, reject) => {
      const proc = spawn(binPath, ["--version"], {
        stdio: ["ignore", "pipe", "pipe"],
        timeout: 5000,
      });

      proc.on("error", reject);
      proc.on("close", (code) => {
        if (code === 0) resolve();
        else reject(new Error(`exit code ${code}`));
      });

      // Also reject if it takes too long
      setTimeout(() => {
        try { proc.kill(); } catch { /* ignore */ }
        reject(new Error("timeout"));
      }, 4000);
    });
  }
}

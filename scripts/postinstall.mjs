#!/usr/bin/env node
/**
 * pi-libp2p-mesh postinstall script.
 *
 * Ensures the ChromaDB Python server is installed so the extension's
 * auto-start feature works out of the box. Runs after `pi install`
 * or `npm install`.
 *
 * Non-blocking: if pip is not available, just prints a helpful message.
 */

import { execSync } from "node:child_process";

const PREFIX = "[pi-libp2p-mesh]";

function log(msg) {
  console.log(`${PREFIX} ${msg}`);
}

function warn(msg) {
  console.warn(`${PREFIX} ${msg}`);
}

try {
  execSync("chroma --version", { stdio: "pipe" });
  log("chromadb already installed");
  process.exit(0);
} catch {
  // chroma not found — try to install
}

log("Installing ChromaDB (Python server)…");

// Try pip3 first, then pip
const pips = ["pip3", "pip"];
let installed = false;

for (const pip of pips) {
  try {
    execSync(`${pip} install chromadb`, { stdio: "inherit" });
    log("chromadb installed successfully");
    installed = true;
    break;
  } catch {
    continue;
  }
}

if (!installed) {
  warn(
    "Could not install chromadb automatically. " +
      "Please install it manually: pip install chromadb",
  );
  // Don't fail the npm install — ChromaDB is optional at install time
}

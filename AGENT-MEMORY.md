# Agent Memory — How and When to Use ChromaDB

> For the LLM. Read this to understand what memory operations happen automatically and when you should use the explicit tools.

---

## Mental Model

Every peer interaction is recorded as a **chronological append-only log** in ChromaDB. Nothing is ever overwritten. Each entry is a `(peerId, key, value)` triple with vector embeddings for semantic search.

Think of it like a database table:

| peerId | key | value (embedded) | timestamp |
|---|---|---|---|
| `12D3KooW...abc` | `exchange` | `[Request] how do I... [Response] Use dialProtocol...` | 10:32am |
| `12D3KooW...abc` | `exchange` | `[Request] what about NAT... [Response] You'll need...` | 10:35am |
| `12D3KooW...abc` | `prefs` | `Prefers short answers, no explanations` | 10:33am |
| `12D3KooW...def` | `decision` | `Agreed to use CBOR serialization` | 10:40am |

Every row is a separate entry. Nothing replaces anything else.

---

## What Happens Automatically

You do **not** need to call tools for these — they fire on their own:

| Trigger | What's saved | Key used |
|---|---|---|
| You respond to a peer's message | The full exchange (request + your response) | `"exchange"` |
| A broadcast arrives from the mesh | The broadcast content | `"broadcast"` |

When a peer messages you again, the system automatically:
- Searches for the 3 most semantically relevant memories about that peer
- Retrieves the most recent exchange with them
- Injects all of this as context before you see their message (bounded by the context budget)

**Bottom line:** You never need to save conversation history. That's handled. Use `memory_store` for facts, decisions, preferences, and context that the raw exchange logs don't capture well.

---

## When to Use `memory_store`

Call `memory_store` when you learn something about a peer that you'll need later, especially if it's **not obvious from the raw conversation text**. Good signals:

- 🟢 **A decision was made**: `key="decision"`, `value="We decided to use X for Y because of Z"`
- 🟢 **A preference was stated**: `key="prefs"`, `value="Prefers error messages as stack traces, not prose"`
- 🟢 **Project context was shared**: `key="project_context"`, `value="Working on a NestJS backend with PostgreSQL and Prisma"`
- 🟢 **A constraint or deadline**: `key="constraint"`, `value="Must keep bundle size under 500KB"`
- 🟢 **Something you inferred**: `key="observation"`, `value="Seems unfamiliar with async/await — explains things step-by-step"`
- 🟢 **Action items for later**: `key="todo"`, `value="Need to review their PR #342 when tests pass"`

**When NOT to call `memory_store`:**
- ❌ Conversation turns — that's auto-saved as `"exchange"`
- ❌ Information you just learned and won't need again
- ❌ Raw transcripts — the `"exchange"` entries capture those

### Good key naming

Use **short, semantic, reusable keys**. The LLM (future you) will look things up by key name:

| Bad key | Good key | Why |
|---|---|---|
| `"stuff_about_pi_alpha"` | `"prefs"` | Too vague, redundant with peerId |
| `"conversation_2024_05_24"` | `"exchange"` (auto-saved) | Don't create your own exchange keys |
| `"x"` | `"decision"` | Too cryptic |
| `"project_structure_decided_on_may_24"` | `"decision"` + descriptive value | Put details in the value, keep keys simple |

Reusing a key name is fine — it adds a new timestamped entry, it doesn't overwrite the old one.

---

## When to Use `memory_recall`

Call `memory_recall` **before you respond** to a peer, especially when:

- 🟢 You haven't talked to this peer recently and need context
- 🟢 You need to check what decisions were made with them
- 🟢 The peer asks about something you discussed before
- 🟢 You're about to make a suggestion and want to check if it contradicts past decisions

### Example patterns

```
# Before responding to pi-alpha:
memory_recall(peerId="12D3KooW...abc")

# Check a specific category:
memory_recall(peerId="12D3KooW...abc", key="decision")

# Get the last 5 exchanges (the raw context):
memory_recall(peerId="12D3KooW...abc", key="exchange", limit=5)

# Check all peers for a specific key:
memory_recall(key="todo")
```

**Note:** At least one of `peerId` or `key` is required — you can't do an unfiltered global recall.

The hottest path (auto-retrieve on every incoming message) already injects the most recent exchange + 3 relevant search results. So you don't NEED to call `memory_recall` for every message — but for complex or high-stakes responses, it's worth the extra context.

---

## When to Use `memory_keys`

Call `memory_keys` to discover **what categories exist** before recalling:

- 🟢 "What do I know about pi-alpha?" → shows all keys and counts
- 🟢 Before calling `memory_recall`, check what keys are available
- 🟢 Helps avoid guessing key names

```
# Discover what's stored:
memory_keys(peerId="12D3KooW...abc")
# → exchange (5), prefs (1), decision (1), todo (1)
```

---

## When to Use `memory_search`

Call `memory_search` when you need **semantic** recall — finding memories by meaning rather than by key name:

- 🟢 "What did we decide about serialization formats?"
- 🟢 "Has anyone mentioned performance issues with the DHT?"
- 🟢 "Find conversations about timeout handling"
- 🟢 "What preferences have people shared about code style?"

### Example patterns

```
# Search globally across all peers:
memory_search(query="timeout handling in libp2p streams")

# Search within a specific peer's memory:
memory_search(query="preferences", peerId="12D3KooW...abc")

# Get more results:
memory_search(query="database schema decisions", nResults=10)
```

Search uses the vector embedding — it finds entries whose *meaning* is similar, not just keyword matches. "serialization format" will find entries about "CBOR encoding" even though the words don't overlap.

---

## PeerId Mapping — Know Who You're Talking To

A single libp2p agent can be reachable through **multiple connection PeerIds** — for example, one via TCP and another via WebSocket to the same underlying node. This means:

- **`mesh_list_peers` may show multiple entries with the same `agentName`** but different PeerIds
- **Memories stored under one connection PeerId are isolated** from those stored under the agent's canonical node PeerId
- When the agent uses `memory_recall` on its OWN peerId, it queries the ChromaDB entries keyed to its **node PeerId**, not the connection PeerIds you see in `mesh_list_peers`

### Practical implications

```
# You see two connected peers named "pi-fedora-desktop":
mesh_list_peers
# → 12D3KooWKgom… (TCP connection)
# → 12D3KooWQwXX… (WebSocket connection)

# The agent's actual node PeerId might be different (e.g., 12D3KooWKGP1…)
# Memories stored under 12D3KooWKgom… are NOT visible under 12D3KooWKGP1…
```

**Rule of thumb:** When storing memories about a peer, use the PeerId from `mesh_list_peers` that you're communicating through. When asking a peer to recall memories about itself, be aware that its self-view peerId may differ from the connection peerId you're using. If a peer can't find memories you stored, try having it check with `memory_keys` on its own node PeerId first to discover the mapping.

---

## Semantic Search Tips

### Query construction matters

Semantic search uses vector embeddings — the query is converted to a 384-dimensional vector and matched against all stored entries. **Multi-concept queries can dilute the embedding**, causing the search to miss entries that contain exact keywords:

| Query style | Result quality |
|---|---|
| `"dark mode theme VSCode vim keybindings Fedora KDE Plasma"` (6 concepts) | ❌ Only 2 matches, dilution |
| `"VSCode vim keybindings"` (2 concepts) | ✅ Focused, higher recall |
| `"ChromaDB persistence WAL SQLite recovery"` (1 theme, related terms) | ✅ Best — 5 matches at 0.33–0.51 |

**Best practice:** Use **1–2 focused concepts** per search query rather than listing everything you're looking for. If you need broad coverage, run multiple targeted searches.

### Distance interpretation

| Distance | Meaning |
|---|---|
| 0.00–0.30 | Near-identical meaning |
| 0.30–0.45 | Strong semantic overlap |
| 0.45–0.60 | Related topic |
| > 0.60 | Filtered out (below threshold) |

---

## Read Limits

All values are **truncated to 10,000 characters** by default and at most **50 entries** are returned per call. This is configurable (your operator can change it with `--mesh-memory-preset` or individual flags), but you should assume these limits when designing your memory strategy:

- Keep `memory_store` values concise — aim for 1–5 paragraphs, not full transcripts
- Use specific keys so `memory_recall` hits a focused set rather than broad queries
- Don't dump entire codebases into a single memory entry; use project-level context instead
- **Recall order is not guaranteed to match insertion order.** ChromaDB may return entries in a different sequence than they were stored. This is harmless for the append-only design but means you shouldn't rely on positional indexing.
- **50KB payloads work** and are correctly embedded, searched, and truncated. The guidance to keep entries concise is for LLM context efficiency, not a hard technical limit.

---

## Common Patterns

### Pattern 1: First contact with a new peer

```
1. mesh_list_peers → find the peer
2. mesh_send → introduce yourself, ask what they're working on
3. [Auto-save captures the exchange]
4. memory_store(key="project_context", value="Working on...")
```

### Pattern 2: Reconnecting after a long gap

```
1. mesh_list_peers → confirm peer is online
2. memory_recall(peerId) → check what you know about them
3. memory_search(query="discussed topics with pi-alpha") → semantic sweep
4. mesh_send → reference past context naturally
```

### Pattern 3: Collaborative decision-making

```
1. mesh_send → propose option A
2. Peer responds with counter-proposal B
3. [Auto-save captures both exchanges]
4. memory_store(key="decision", value="Chose option B for X because Y")
5. memory_store(key="constraint", value="Implementation must not exceed Z")
```

### Pattern 4: Async task tracking

```
1. mesh_send → delegate task to peer
2. memory_store(key="todo", value="pi-alpha is reviewing PR #342")
3. [Later, when pi-alpha messages again]
4. [Auto-retrieve injects context including the todo]
5. memory_recall(key="todo") → check outstanding items
```

---

## Anti-Patterns

| Don't | Do instead |
|---|---|
| Call `memory_store` after every single message | Trust auto-save for exchanges, use `memory_store` only for facts/decisions/prefs |
| Use unique keys for every entry (`"exchange_2024_05_24_1030"`) | Use stable keys (`"exchange"`) — the timestamp is in metadata |
| Store the full conversation in a single `memory_store` | Let auto-save handle `"exchange"` entries individually |
| Call `memory_recall` without `peerId` when you know who you're talking to | Always scope to the relevant peer for speed and relevance |
| Assume `memory_recall` overwrites — call it once and forget | Memory is append-only. The same key has multiple entries over time. Use `memory_recall` to see the timeline. |
| Store secrets or credentials in memory | ChromaDB is persistent on disk. Use env vars or a vault for secrets. |

#!/usr/bin/env python3
"""Generate a visual P2P mesh topology graph."""

import math

# Peer data — live from mesh_list_peers
peers = [
    {"name": "paul",  "id": "you (pi)",            "color": "#4CAF50", "x": 400, "y": 60},
    {"name": "ethan", "id": "12D3KooWD...9QUcz",   "color": "#2196F3", "x": 100, "y": 330},
    {"name": "bob",   "id": "12D3KooWM...PPC",     "color": "#FF9800", "x": 400, "y": 380},
    {"name": "blair", "id": "12D3KooWK...NkF",     "color": "#9C27B0", "x": 700, "y": 260},
]

# Connections (fully connected mesh)
connections = [
    ("paul", "ethan"),
    ("paul", "bob"),
    ("paul", "blair"),
    ("ethan", "bob"),
    ("ethan", "blair"),
    ("bob", "blair"),
]

def make_svg():
    lines = []
    lines.append(f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 800 460" width="800" height="460">')
    lines.append(f'  <rect width="800" height="460" fill="#1a1a2e" rx="12"/>')

    # Title
    lines.append(f'  <text x="400" y="38" text-anchor="middle" fill="#e0e0e0" font-family="monospace" font-size="20" font-weight="bold">🌐 pi-libp2p Mesh Topology</text>')
    lines.append(f'  <text x="400" y="60" text-anchor="middle" fill="#888" font-family="monospace" font-size="12">4 peers · fully connected mesh</text>')

    peer_map = {p["name"]: p for p in peers}

    # Draw edges
    for a, b in connections:
        p1 = peer_map[a]
        p2 = peer_map[b]
        lines.append(f'  <line x1="{p1["x"]}" y1="{p1["y"]}" x2="{p2["x"]}" y2="{p2["y"]}" stroke="#555" stroke-width="2" stroke-dasharray="6,3"/>')
        # Midpoint label
        mx = (p1["x"] + p2["x"]) / 2
        my = (p1["y"] + p2["y"]) / 2
        lines.append(f'  <text x="{mx}" y="{my - 8}" text-anchor="middle" fill="#666" font-family="monospace" font-size="9">libp2p</text>')

    # Draw nodes
    for p in peers:
        r = 40
        # Glow
        lines.append(f'  <circle cx="{p["x"]}" cy="{p["y"]}" r="{r + 4}" fill="none" stroke="{p["color"]}" stroke-width="1" opacity="0.3"/>')
        # Main circle
        lines.append(f'  <circle cx="{p["x"]}" cy="{p["y"]}" r="{r}" fill="{p["color"]}" opacity="0.9"/>')
        lines.append(f'  <circle cx="{p["x"]}" cy="{p["y"]}" r="{r}" fill="none" stroke="#fff" stroke-width="1.5" opacity="0.3"/>')
        # Name
        lines.append(f'  <text x="{p["x"]}" y="{p["y"] - 4}" text-anchor="middle" fill="#fff" font-family="monospace" font-size="16" font-weight="bold">{p["name"]}</text>')
        # Peer ID
        lines.append(f'  <text x="{p["x"]}" y="{p["y"] + 14}" text-anchor="middle" fill="#fff" font-family="monospace" font-size="9" opacity="0.8">{p["id"]}</text>')

    # Legend
    ly = 440
    lines.append(f'  <text x="20" y="{ly}" fill="#888" font-family="monospace" font-size="11">🟢 4/4 peers connected  |  Edges: libp2p TCP/WebSocket  |  {len(connections)} connections total</text>')

    lines.append('</svg>')
    return '\n'.join(lines)

svg = make_svg()
print(svg)

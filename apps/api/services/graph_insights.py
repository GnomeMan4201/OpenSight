from __future__ import annotations

from collections import defaultdict
from typing import Any

import networkx as nx


def build_nx_graph(rels) -> nx.Graph:
    G = nx.Graph()
    for r in rels:
        a = str(getattr(r, "entity_a_id", "") or "")
        b = str(getattr(r, "entity_b_id", "") or "")
        if not a or not b:
            continue
        w = float(getattr(r, "weight", 1) or 1)
        G.add_edge(a, b, weight=w, inv_weight=(1.0 / max(w, 1.0)))
    return G


def top_broker_paths(G: nx.Graph, entity_id: str, limit: int = 8) -> list[dict[str, Any]]:
    if entity_id not in G:
        return []

    nodes = list(G.nodes())
    paths: list[dict[str, Any]] = []
    seen = set()

    for src in nodes:
        if src == entity_id:
            continue
        for dst in nodes:
            if dst == entity_id or dst == src:
                continue
            try:
                path = nx.shortest_path(G, src, dst, weight="inv_weight")
            except nx.NetworkXNoPath:
                continue
            except nx.NodeNotFound:
                continue

            if entity_id not in path:
                continue

            if path[0] > path[-1]:
                path = list(reversed(path))

            key = tuple(path)
            if key in seen:
                continue
            seen.add(key)

            idx = path.index(entity_id)
            bridge_span = min(idx, len(path) - 1 - idx)
            score = len(path) + bridge_span

            paths.append({
                "path": path,
                "length": len(path) - 1,
                "bridge_position": idx,
                "bridge_score": score,
            })

    paths.sort(key=lambda x: (x["bridge_score"], -x["length"]), reverse=True)
    return paths[:limit]


def graph_communities(G: nx.Graph) -> dict[str, Any]:
    comms = list(nx.algorithms.community.greedy_modularity_communities(G, weight="weight"))
    membership: dict[str, int] = {}
    summary: list[dict[str, Any]] = []

    for idx, comm in enumerate(comms, start=1):
        members = list(comm)
        for nid in members:
            membership[str(nid)] = idx
        summary.append({
            "community_id": idx,
            "size": len(members),
            "members": [str(n) for n in members[:50]],
        })

    summary.sort(key=lambda x: x["size"], reverse=True)
    return {
        "community_count": len(summary),
        "membership": membership,
        "communities": summary,
    }

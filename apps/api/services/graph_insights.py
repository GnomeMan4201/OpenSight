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
    seen: set[tuple] = set()

    for src in nodes:
        if src == entity_id:
            continue
        for dst in nodes:
            if dst == entity_id or dst == src:
                continue
            try:
                path = nx.shortest_path(G, src, dst, weight="inv_weight")
            except (nx.NetworkXNoPath, nx.NodeNotFound):
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


def graph_communities(
    G: nx.Graph,
    entity_map: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Detect communities and return rich named-entity summaries.
    entity_map: {entity_id: Entity ORM object} — optional, enables name enrichment.
    """
    if G.number_of_nodes() == 0:
        return {"community_count": 0, "membership": {}, "communities": []}

    comms = list(nx.algorithms.community.greedy_modularity_communities(G, weight="weight"))

    # Betweenness for key actor detection
    try:
        bc = nx.betweenness_centrality(G, weight="weight", normalized=True)
    except Exception:
        bc = {n: 0.0 for n in G.nodes()}

    membership: dict[str, int] = {}
    summary: list[dict[str, Any]] = []

    for idx, comm in enumerate(comms, start=1):
        members = list(comm)
        for nid in members:
            membership[str(nid)] = idx

        # Density of the subgraph
        subG = G.subgraph(members)
        density = round(nx.density(subG), 4)

        # Key actor = highest betweenness within community
        key_actor_id = max(members, key=lambda n: bc.get(n, 0))

        # Enrich member list with entity details if map provided
        member_details = []
        for nid in sorted(members, key=lambda n: -bc.get(n, 0)):
            detail: dict[str, Any] = {"id": str(nid)}
            if entity_map and nid in entity_map:
                e = entity_map[nid]
                detail["name"] = e.canonical_name
                detail["entity_type"] = e.entity_type
                detail["mention_count"] = e.mention_count
                detail["betweenness"] = round(bc.get(nid, 0), 4)
            else:
                detail["name"] = str(nid)
                detail["entity_type"] = "unknown"
                detail["mention_count"] = 0
                detail["betweenness"] = round(bc.get(nid, 0), 4)
            member_details.append(detail)

        key_actor_name = entity_map[key_actor_id].canonical_name if (entity_map and key_actor_id in entity_map) else key_actor_id

        summary.append({
            "community_id": idx,
            "size": len(members),
            "density": density,
            "key_actor": key_actor_name,
            "key_actor_id": str(key_actor_id),
            "members": member_details,
        })

    summary.sort(key=lambda x: x["size"], reverse=True)
    return {
        "community_count": len(summary),
        "membership": membership,
        "communities": summary,
    }

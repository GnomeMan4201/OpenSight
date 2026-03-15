from __future__ import annotations
import networkx as nx

def build_case_graph(case: dict) -> dict:
    g = nx.Graph()

    for entity in case.get("entities", []):
        node_id = f'{entity["type"]}:{entity["value"]}'
        g.add_node(node_id, label=entity["value"], entity_type=entity["type"], frequency=entity.get("frequency", 1))

    docs = case.get("documents", [])
    for doc in docs:
        doc_id = f'doc:{doc["id"]}'
        g.add_node(doc_id, label=doc["title"], entity_type="document")
        for entity in doc.get("entities", []):
            node_id = f'{entity["type"]}:{entity["value"]}'
            if g.has_node(node_id):
                g.add_edge(doc_id, node_id, relation="mentions")

    return {
        "nodes": [
            {"id": n, **g.nodes[n]}
            for n in g.nodes
        ],
        "edges": [
            {"source": a, "target": b, **g.edges[a, b]}
            for a, b in g.edges
        ],
        "stats": {
            "node_count": g.number_of_nodes(),
            "edge_count": g.number_of_edges(),
        }
    }

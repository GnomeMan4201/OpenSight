from __future__ import annotations
from pyvis.network import Network
from pathlib import Path

def write_graph_html(graph_payload: dict, out_path: str = "ui/investigation/graph_preview.html") -> str:
    net = Network(height="800px", width="100%", bgcolor="#0b1020", font_color="white")
    for node in graph_payload["nodes"]:
        net.add_node(node["id"], label=node.get("label", node["id"]), title=node.get("entity_type", "node"))
    for edge in graph_payload["edges"]:
        net.add_edge(edge["source"], edge["target"], title=edge.get("relation", "related"))
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    net.write_html(out_path)
    return out_path

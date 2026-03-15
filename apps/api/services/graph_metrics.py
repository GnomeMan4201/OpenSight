import networkx as nx


def compute_graph_metrics(nodes, edges):
    G = nx.Graph()

    for node in nodes:
        node_id = node.get("id")
        if node_id is not None:
            G.add_node(node_id)

    for edge in edges:
        src = edge.get("source")
        tgt = edge.get("target")
        if src is None or tgt is None:
            continue
        G.add_edge(src, tgt)

    if G.number_of_nodes() == 0:
        return {
            "node_count": 0,
            "edge_count": 0,
            "degree": {},
            "betweenness": {},
            "eigenvector": {},
        }

    degree = nx.degree_centrality(G)
    betweenness = nx.betweenness_centrality(G)

    try:
        eigenvector = nx.eigenvector_centrality(G, max_iter=1000)
    except Exception:
        eigenvector = {str(n): 0.0 for n in G.nodes()}

    return {
        "node_count": G.number_of_nodes(),
        "edge_count": G.number_of_edges(),
        "degree": {str(k): v for k, v in degree.items()},
        "betweenness": {str(k): v for k, v in betweenness.items()},
        "eigenvector": {str(k): v for k, v in eigenvector.items()},
    }

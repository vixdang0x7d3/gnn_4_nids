import marimo

__generated_with = "0.18.0"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo

    import sys
    import os

    import torch

    import duckdb
    return duckdb, mo, os, sys, torch


@app.cell
def _(sys):
    cwd = "/home/v/works/gnn_4_nids/model_training"
    if cwd not in sys.path:
        sys.path.insert(0, "/home/v/works/gnn_4_nids/model_training")
    return


@app.cell
def _(sys):
    sys.path
    return


@app.cell
def _():
    from src.gnn.nb15_gcn import NB15_GCN
    from src.gnn.const import FEATURE_ATTRIBUTES, EDGE_ATTRIBUTES, MULTICLASS_LABEL

    from graph_building import minmax_scale, groupwise_smote
    from graph_building import GraphGCN
    return (
        EDGE_ATTRIBUTES,
        FEATURE_ATTRIBUTES,
        GraphGCN,
        MULTICLASS_LABEL,
        groupwise_smote,
        minmax_scale,
    )


@app.cell
def _(os):
    _ = os.system("ls -l ./data/NB15_preprocessed/raw")
    return


@app.cell
def _(duckdb):
    data = duckdb.read_parquet("./data/NB15_preprocessed/raw/train.parquet")
    return (data,)


@app.cell
def _(data, mo):
    _df = mo.sql(
        f"""
        select multiclass_label, count(*) as n_samples
        from data
        group by multiclass_label
        """
    )
    return


@app.cell
def _(data, mo):
    tripplet_group_counts = mo.sql(
        f"""
        SELECT CONCAT_WS(', ', proto, state, service) as group_name, COUNT(*) as group_size FROM data
        GROUP BY proto, state, service
        """
    )
    return


@app.cell
def _(data, mo):
    _df = mo.sql(
        f"""
        SELECT 
        	concat_ws('-', proto, state, service) as group_name,
            COUNT(*) as total_flows,
            SUM(CASE WHEN attack_cat = 'Exploits' THEN 1 ELSE 0 END) as exploits_count,
            SUM(CASE WHEN attack_cat = 'Backdoors' THEN 1 ELSE 0 END) as backdoors_count,
            SUM(CASE WHEN attack_cat = 'Shellcode' THEN 1 ELSE 0 END) as shellcode_count,
            SUM(CASE WHEN attack_cat = 'Worms' THEN 1 ELSE 0 END) as worms_count,
            SUM(CASE WHEN attack_cat = 'DoS' THEN 1 ELSE 0 END) as dos_count,
            SUM(CASE WHEN attack_cat = 'Fuzzers' THEN 1 ELSE 0 END) as fuzzers_count,
            SUM(CASE WHEN attack_cat = 'Reconnaissance' THEN 1 ELSE 0 END) as recon_count,
            SUM(CASE WHEN attack_cat = 'Generic' THEN 1 ELSE 0 END) as generic_count,
            SUM(CASE WHEN attack_cat = 'Normal' THEN 1 ELSE 0 END) as normal_count,
            SUM(CASE WHEN attack_cat = 'Analysis' THEN 1 ELSE 0 END) as anal_count,
        FROM data
        GROUP BY proto, state, service
        ORDER BY total_flows DESC;
        """
    )
    return


@app.cell
def _(EDGE_ATTRIBUTES, FEATURE_ATTRIBUTES, MULTICLASS_LABEL, data):
    raw_table = (
        data.order("stime")
        .select(
            f"ROW_NUMBER() OVER () as index, {', '.join(FEATURE_ATTRIBUTES)}, {', '.join(EDGE_ATTRIBUTES)}, {MULTICLASS_LABEL}"
        )
        .arrow()
        .read_all()
    )

    raw_table
    return (raw_table,)


@app.cell
def _(
    EDGE_ATTRIBUTES,
    FEATURE_ATTRIBUTES,
    groupwise_smote,
    minmax_scale,
    raw_table,
):
    _table, fitted_scaler = minmax_scale(raw_table, FEATURE_ATTRIBUTES)
    transformed_table = groupwise_smote(
        _table, FEATURE_ATTRIBUTES, EDGE_ATTRIBUTES, 100
    )
    return (transformed_table,)


@app.cell
def _(transformed_table):
    transformed_table
    return


@app.cell
def _(mo, transformed_table):
    _df = mo.sql(
        f"""
        select multiclass_label, count(*) as n_samples from transformed_table
        group by multiclass_label
        order by n_samples
        """
    )
    return


@app.cell
def _(EDGE_ATTRIBUTES, FEATURE_ATTRIBUTES, GraphGCN, transformed_table):
    inp_graph = GraphGCN(
        transformed_table,
        node_attrs=FEATURE_ATTRIBUTES,
        edge_attrs=EDGE_ATTRIBUTES,
        label="multiclass_label",
        n_neighbors=2,
    )

    ptg = inp_graph.build(include_labels=True)
    return (ptg,)


@app.cell
def _():
    import matplotlib.pyplot as plt
    import seaborn as sns
    import numpy as np
    return np, plt, sns


@app.cell
def _(np, plt, ptg, sns, torch):
    if ptg is not None:
        # Basic graph statistics visualization
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))

        # 1. Node degree distribution
        if ptg.edge_index.size(1) > 0:
            node_degrees = torch.zeros(ptg.num_nodes, dtype=torch.long)
            node_degrees.scatter_add_(
                0,
                ptg.edge_index[0],
                torch.ones(ptg.edge_index.size(1), dtype=torch.long),
            )
            node_degrees.scatter_add_(
                0,
                ptg.edge_index[1],
                torch.ones(ptg.edge_index.size(1), dtype=torch.long),
            )

            axes[0, 0].hist(node_degrees.numpy(), bins=30, alpha=0.7)
            axes[0, 0].set_title("Node Degree Distribution")
            axes[0, 0].set_xlabel("Degree")
            axes[0, 0].set_ylabel("Count")
        else:
            axes[0, 0].text(0.5, 0.5, "No edges in graph", ha="center", va="center")
            axes[0, 0].set_title("Node Degree Distribution")

        # 2. Label distribution
        graph_labels = ptg.y.numpy()
        unique_labels, counts = np.unique(graph_labels, return_counts=True)
        axes[0, 1].bar(unique_labels, counts, alpha=0.7)
        axes[0, 1].set_title("Label Distribution")
        axes[0, 1].set_xlabel("Label")
        axes[0, 1].set_ylabel("Count")

        # 3. Feature distribution heatmap
        feature_data = ptg.x.numpy()
        if feature_data.shape[1] <= 20:  # Only if reasonable number of features
            sns.heatmap(
                np.corrcoef(feature_data.T), ax=axes[1, 0], cmap="coolwarm", center=0
            )
            axes[1, 0].set_title("Feature Correlation Matrix")
        else:
            axes[1, 0].text(
                0.5,
                0.5,
                f"{feature_data.shape[1]} features\n(too many for heatmap)",
                ha="center",
                va="center",
            )
            axes[1, 0].set_title("Feature Correlation Matrix")

        # 4. Graph connectivity
        if ptg.edge_index.size(1) > 0:
            edge_density = ptg.edge_index.size(1) / (
                ptg.num_nodes * (ptg.num_nodes - 1)
            )
            connected_components = 1  # Simplified estimate

            metrics_text = f"""
            Nodes: {ptg.num_nodes:,}
            Edges: {ptg.edge_index.size(1):,}
            Features: {ptg.num_features}
            Classes: {ptg.num_node_types}
            Edge Density: {edge_density:.6f}
            """
            axes[1, 1].text(0.1, 0.5, metrics_text, fontsize=12, va="center")
        else:
            metrics_text = f"""
            Nodes: {ptg.num_nodes:,}
            Edges: 0
            Features: {ptg.num_features}
            Classes: {ptg.num_node_types}
            Graph: Disconnected (no edges)
            """
            axes[1, 1].text(0.1, 0.5, metrics_text, fontsize=12, va="center")

        axes[1, 1].set_title("Graph Statistics")
        axes[1, 1].axis("off")

        plt.tight_layout()
        plt.show()

        print("Graph visualization saved as dataset_visualization.png")
        print(f"Graph data shape: nodes={ptg.num_nodes}, features={ptg.num_features}")
        print(f"Edge connectivity: {ptg.edge_index.size(1)} edges")
    else:
        print("No graph data to visualize")
    return


@app.cell
def _():
    from sklearn.decomposition import PCA
    from sklearn.manifold import TSNE

    import networkx as nx
    return PCA, TSNE, nx


@app.cell
def _(PCA, TSNE, np, nx, plt, ptg, torch):
    if ptg is not None and ptg.edge_index.size(1) > 0:
        print("Creating graph structure visualizations...")

        # Sample data for visualization (use subset for large graphs)
        max_nodes_viz = 5000
        if ptg.num_nodes > max_nodes_viz:
            # Sample nodes while preserving label distribution
            node_labels = ptg.y.numpy()
            unique_node_labels = np.unique(node_labels)
            sample_indices = []

            samples_per_label = max_nodes_viz // len(unique_node_labels)
            for label in unique_node_labels:
                label_indices = np.where(node_labels == label)[0]
                if len(label_indices) > samples_per_label:
                    selected = np.random.choice(
                        label_indices, samples_per_label, replace=False
                    )
                else:
                    selected = label_indices
                sample_indices.extend(selected)

            sample_indices = np.array(sample_indices[:max_nodes_viz])

            # Create subgraph
            node_mask = torch.zeros(ptg.num_nodes, dtype=torch.bool)
            node_mask[sample_indices] = True

            # Filter edges to only include sampled nodes
            edge_mask = node_mask[ptg.edge_index[0]] & node_mask[ptg.edge_index[1]]
            sampled_edges = ptg.edge_index[:, edge_mask]

            # Remap node indices
            old_to_new = torch.full((ptg.num_nodes,), -1, dtype=torch.long)
            old_to_new[sample_indices] = torch.arange(len(sample_indices))
            sampled_edges = old_to_new[sampled_edges]

            sample_x = ptg.x[sample_indices]
            sample_y = ptg.y[sample_indices]

            print(
                f"Sampled {len(sample_indices)} nodes from {ptg.num_nodes} for visualization"
            )
        else:
            sampled_edges = ptg.edge_index
            sample_x = ptg.x
            sample_y = ptg.y
            sample_indices = np.arange(ptg.num_nodes)

        # Create figure for graph structure visualizations
        struct_fig, struct_axes = plt.subplots(2, 2, figsize=(16, 16))

        # 1. PCA visualization
        if sample_x.shape[0] > 1:
            pca = PCA(n_components=2)
            pca_result = pca.fit_transform(sample_x.numpy())

            scatter = struct_axes[0, 0].scatter(
                pca_result[:, 0],
                pca_result[:, 1],
                c=sample_y.numpy(),
                cmap="tab10",
                alpha=0.6,
                s=20,
            )
            struct_axes[0, 0].set_title(
                f"PCA Visualization\nExplained Variance: {pca.explained_variance_ratio_.sum():.2f}"
            )
            struct_axes[0, 0].set_xlabel(
                f"PC1 ({pca.explained_variance_ratio_[0]:.2f})"
            )
            struct_axes[0, 0].set_ylabel(
                f"PC2 ({pca.explained_variance_ratio_[1]:.2f})"
            )
            plt.colorbar(scatter, ax=struct_axes[0, 0], label="Class")

        # 2. t-SNE visualization (sample smaller subset for t-SNE due to computational cost)
        tsne_max = min(1000, len(sample_x))
        if tsne_max > 50:  # Only run t-SNE if we have enough samples
            tsne_indices = np.random.choice(len(sample_x), tsne_max, replace=False)
            tsne_x = sample_x[tsne_indices].numpy()
            tsne_y = sample_y[tsne_indices].numpy()

            print(f"Running t-SNE on {tsne_max} samples...")
            tsne = TSNE(
                n_components=2, random_state=42, perplexity=min(30, tsne_max - 1)
            )
            tsne_result = tsne.fit_transform(tsne_x)

            scatter = struct_axes[0, 1].scatter(
                tsne_result[:, 0],
                tsne_result[:, 1],
                c=tsne_y,
                cmap="tab10",
                alpha=0.6,
                s=20,
            )
            struct_axes[0, 1].set_title("t-SNE Visualization")
            struct_axes[0, 1].set_xlabel("t-SNE 1")
            struct_axes[0, 1].set_ylabel("t-SNE 2")
            plt.colorbar(scatter, ax=struct_axes[0, 1], label="Class")
        else:
            struct_axes[0, 1].text(
                0.5, 0.5, "Insufficient data for t-SNE", ha="center", va="center"
            )
            struct_axes[0, 1].set_title("t-SNE Visualization")

        # 3. Network graph visualization (sample connected subgraph)
        net_max = min(200, len(sample_x))  # Reduced for better connectivity density
        if sampled_edges.size(1) > 0 and net_max > 10:
            # Strategy: Find edges first, then select nodes that participate in those edges
            if len(sample_x) > net_max:
                # Method 1: Select edges with highest degree endpoints
                temp_degrees = torch.zeros(len(sample_x), dtype=torch.long)
                temp_degrees.scatter_add_(
                    0,
                    sampled_edges[0],
                    torch.ones(sampled_edges.size(1), dtype=torch.long),
                )
                temp_degrees.scatter_add_(
                    0,
                    sampled_edges[1],
                    torch.ones(sampled_edges.size(1), dtype=torch.long),
                )

                # Score edges by sum of endpoint degrees
                edge_scores = (
                    temp_degrees[sampled_edges[0]] + temp_degrees[sampled_edges[1]]
                )

                # Sort edges by score (descending)
                sorted_edge_indices = torch.argsort(edge_scores, descending=True)

                # Select top edges and their nodes
                selected_nodes = set()
                selected_edge_indices = []

                for edge_idx in sorted_edge_indices:
                    src, dst = (
                        sampled_edges[0, edge_idx].item(),
                        sampled_edges[1, edge_idx].item(),
                    )

                    # Add edge if it doesn't make us exceed node limit
                    potential_new_nodes = {src, dst} - selected_nodes
                    if len(selected_nodes) + len(potential_new_nodes) <= net_max:
                        selected_nodes.update({src, dst})
                        selected_edge_indices.append(edge_idx.item())
                    elif (
                        len(selected_nodes) < net_max and len(potential_new_nodes) == 1
                    ):
                        # Add edge if only one new node
                        selected_nodes.update(potential_new_nodes)
                        selected_edge_indices.append(edge_idx.item())

                    if len(selected_nodes) >= net_max:
                        break

                # If we still don't have enough nodes, add high-degree isolated nodes
                if len(selected_nodes) < net_max:
                    remaining_nodes = set(range(len(sample_x))) - selected_nodes
                    if remaining_nodes:
                        remaining_list = list(remaining_nodes)
                        remaining_degrees = temp_degrees[remaining_list]
                        sorted_remaining = [
                            remaining_list[i]
                            for i in torch.argsort(remaining_degrees, descending=True)
                        ]

                        add_count = min(
                            len(sorted_remaining), net_max - len(selected_nodes)
                        )
                        selected_nodes.update(sorted_remaining[:add_count])

                net_indices = np.array(list(selected_nodes))
                print(
                    f"Selected {len(selected_edge_indices)} high-scoring edges covering {len(net_indices)} nodes"
                )
            else:
                net_indices = np.arange(len(sample_x))

            # Create mapping for network indices
            net_old_to_new = {
                old_idx: new_idx for new_idx, old_idx in enumerate(net_indices)
            }

            # Filter edges for network visualization
            net_edges = []
            for i in range(sampled_edges.size(1)):
                src, dst = sampled_edges[0, i].item(), sampled_edges[1, i].item()
                if src in net_old_to_new and dst in net_old_to_new:
                    net_edges.append((net_old_to_new[src], net_old_to_new[dst]))

            print(
                f"Network graph: selected {len(net_indices)} nodes with {len(net_edges)} edges"
            )

            if net_edges:
                # Create NetworkX graph
                G = nx.Graph()
                G.add_nodes_from(range(len(net_indices)))
                G.add_edges_from(net_edges)

                # Layout with better spacing
                pos = nx.spring_layout(
                    G, k=1 / np.sqrt(len(net_indices) / 10), iterations=50, seed=42
                )

                # Node colors based on labels
                node_colors = [
                    sample_y[net_indices[node]].item()
                    for node in G.nodes()
                    if node < len(net_indices)
                ]

                nx.draw(
                    G,
                    pos,
                    ax=struct_axes[1, 0],
                    node_color=node_colors,
                    node_size=50,
                    alpha=0.8,
                    cmap="tab10",
                    edge_color="gray",
                    width=0.3,
                    with_labels=False,
                )
                struct_axes[1, 0].set_title(
                    f"Network Graph ({len(net_indices)} nodes, {len(net_edges)} edges)"
                )
            else:
                struct_axes[1, 0].text(
                    0.5, 0.5, "No edges in subgraph", ha="center", va="center"
                )
                struct_axes[1, 0].set_title("Network Graph")
        else:
            struct_axes[1, 0].text(
                0.5,
                0.5,
                "Insufficient edges for visualization",
                ha="center",
                va="center",
            )
            struct_axes[1, 0].set_title("Network Graph")

        # 4. Degree vs Feature correlation
        if sampled_edges.size(1) > 0:
            # Calculate degrees for sampled nodes
            sample_degrees = torch.zeros(len(sample_x), dtype=torch.long)
            sample_degrees.scatter_add_(
                0, sampled_edges[0], torch.ones(sampled_edges.size(1), dtype=torch.long)
            )
            sample_degrees.scatter_add_(
                0, sampled_edges[1], torch.ones(sampled_edges.size(1), dtype=torch.long)
            )

            # Use first principal component as representative feature
            if sample_x.shape[0] > 1:
                pca_single = PCA(n_components=1)
                feature_repr = pca_single.fit_transform(sample_x.numpy()).flatten()

                struct_axes[1, 1].scatter(
                    sample_degrees.numpy(), feature_repr, alpha=0.6, s=20
                )
                struct_axes[1, 1].set_xlabel("Node Degree")
                struct_axes[1, 1].set_ylabel("Feature PC1")
                struct_axes[1, 1].set_title("Degree vs Feature Relationship")

                # Add correlation coefficient
                corr = np.corrcoef(sample_degrees.numpy(), feature_repr)[0, 1]
                struct_axes[1, 1].text(
                    0.05,
                    0.95,
                    f"Correlation: {corr:.3f}",
                    transform=struct_axes[1, 1].transAxes,
                    bbox=dict(boxstyle="round", facecolor="white", alpha=0.8),
                )
            else:
                struct_axes[1, 1].text(
                    0.5, 0.5, "Insufficient data", ha="center", va="center"
                )
                struct_axes[1, 1].set_title("Degree vs Feature Relationship")
        else:
            struct_axes[1, 1].text(
                0.5, 0.5, "No edges available", ha="center", va="center"
            )
            struct_axes[1, 1].set_title("Degree vs Feature Relationship")

        plt.tight_layout()
        plt.show()

        print(
            "Graph structure visualization saved as graph_structure_visualization.png"
        )
    else:
        print("No graph structure to visualize (no edges or no graph data)")
    return


if __name__ == "__main__":
    app.run()

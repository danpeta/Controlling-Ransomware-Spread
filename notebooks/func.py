import numpy as np
import scipy as sp
import networkx as nx
from random import sample

def read_betweenness(filename, edgelist):
    values = np.loadtxt(filename, dtype=np.float64)
    assert values.shape[0] == edgelist.shape[0]
    return dict(((line[0],line[1]), val) for line,val in zip(edgelist,values))

def sort_betweenness(betweenness):
    return sorted(betweenness, key=betweenness.get, reverse=True)

def edge_to_node_betweenness(edge_betweenness, nodes):
    node_betweenness = dict((u, 0.) for u in nodes)
    for ((i,j), val) in edge_betweenness.items():
        node_betweenness[i] += val
        node_betweenness[j] += val
    return node_betweenness

def get_target_edges(sorted_edges, target_perc):
    r = int(len(sorted_edges)*target_perc)
    return sorted_edges[:r]

def create_weighted_adjacency(graph, target_edges, weight=.1):
    G = graph.copy()
    weights = dict((e,1.0) for e in G.edges())
    for e in target_edges:
        weights[e] = weight
    nx.set_edge_attributes(G, weights, 'weight')
    return (nx.adjacency_matrix(G, weight='weight') + sp.sparse.eye(G.number_of_nodes())).tocsc()

def create_weighted_adjacency_from_edge_betweenness(graph, edge_betweenness, target_perc, weight=.1):
    target_edges = get_target_edges(sort_betweenness(edge_betweenness), target_perc)
    return create_weighted_adjacency(graph, target_edges, weight=weight)

def create_weighted_adjacency_from_degree_dist(graph, target_perc, weight=.1, descending=True):
    G = graph.copy()
    nodes_sorted = sorted(G.nodes, key=G.degree, reverse=descending)
    weights = dict((e,1.0) for e in G.edges())
    m = G.number_of_edges()
    ct = 0
    for i in nodes_sorted:
        for j in G.neighbors(i):
            try:
                weights[(i,j)] = weight
                ct += 1
            except:
                weights[(j,i)] = weight
                ct += 1
            if ct/m > target_perc:
                break
        if ct/m > target_perc:
                break
    nx.set_edge_attributes(G, weights, 'weight')
    return (nx.adjacency_matrix(G, weight='weight') + sp.sparse.eye(G.number_of_nodes())).tocsc()

def create_weighted_adjacency_from_degree_dist_target_nodes(graph, target_perc, weight=.1, descending=True):
    G = graph.copy()
    nodes_sorted = sorted(G.nodes, key=G.degree, reverse=descending)
    weights = dict((e,1.0) for e in G.edges())
    n = G.number_of_nodes()
    ct = 0
    for i in nodes_sorted:
        for j in G.neighbors(i):
            try:
                weights[(i,j)] = weight
            except:
                weights[(j,i)] = weight
        ct += 1
        if ct/n > target_perc:
                break
    nx.set_edge_attributes(G, weights, 'weight')
    return (nx.adjacency_matrix(G, weight='weight') + sp.sparse.eye(G.number_of_nodes())).tocsc()

def create_weighted_adjacency_from_node_betweenness(graph, node_betweenness, target_perc, weight=.1, descending=True):
    G = graph.copy()
    nodes_sorted = sorted(node_betweenness, key=node_betweenness.get, reverse=descending)
    weights = dict((e,1.0) for e in G.edges())
    m = G.number_of_edges()
    ct = 0
    for i in nodes_sorted:
        for j in G.neighbors(i):
            try:
                weights[(i,j)] = weight
                ct += 1
            except:
                weights[(j,i)] = weight
                ct += 1
            if ct/m > target_perc:
                break
        if ct/m > target_perc:
                break
    nx.set_edge_attributes(G, weights, 'weight')
    return (nx.adjacency_matrix(G, weight='weight') + sp.sparse.eye(G.number_of_nodes())).tocsc()

def create_weighted_adjacency_from_node_betweenness_target_nodes(graph, node_betweenness, target_perc, weight=.1, descending=True):
    G = graph.copy()
    nodes_sorted = sorted(node_betweenness, key=node_betweenness.get, reverse=descending)
    weights = dict((e,1.0) for e in G.edges())
    n = G.number_of_nodes()
    ct = 0
    for i in nodes_sorted:
        for j in G.neighbors(i):
            try:
                weights[(i,j)] = weight
            except:
                weights[(j,i)] = weight
        ct += 1
        if ct/n > target_perc:
                break
    nx.set_edge_attributes(G, weights, 'weight')
    return (nx.adjacency_matrix(G, weight='weight') + sp.sparse.eye(G.number_of_nodes())).tocsc()

def create_weighted_adjacency_target_random_nodes(graph, target_perc, weight=.1):
    G = graph.copy()
    weights = dict((e,1.0) for e in G.edges())
    n = G.number_of_nodes()
    selected_nodes = sample(G.nodes(), int(np.ceil(n*target_perc)))
    for i in selected_nodes:
        for j in G.neighbors(i):
            try:
                weights[(i,j)] = weight
            except:
                weights[(j,i)] = weight
    nx.set_edge_attributes(G, weights, 'weight')
    return (nx.adjacency_matrix(G, weight='weight') + sp.sparse.eye(G.number_of_nodes())).tocsc()

def create_list_of_weights(ll, edge_betweenness, target_perc, weight=.1):
    lw = [[1.0]*len(list_) for list_ in ll]
    edges_sorted = sort_betweenness(edge_betweenness)
    target_edges = get_target_edges(edges_sorted, target_perc)
    for u,v in target_edges:
        lw[u-1][ll[u-1].index(v)] = weight
        lw[v-1][ll[v-1].index(u)] = weight
    return lw

def create_list_of_weights_node_centrality(ll, node_centrality, target_perc, num_edges, weight=.1):
    lw = [[1.0]*len(list_) for list_ in ll]
    nodes_sorted = sorted(node_centrality, key=node_centrality.get, reverse=True)
    ct = 0
    for i in nodes_sorted:
        for j in ll[i-1]:
            if lw[i-1][ll[i-1].index(j)] == weight:
                continue
            else:
                lw[i-1][ll[i-1].index(j)] = weight
                lw[j-1][ll[j-1].index(i)] = weight
                ct += 1
            if ct/num_edges > target_perc:
                break
        if ct/num_edges > target_perc:
            break
    return lw

def create_list_of_weights_node_centrality_target_nodes(ll, node_centrality, target_perc, weight=.1):
    lw = [[1.0]*len(list_) for list_ in ll]
    nodes_sorted = sorted(node_centrality, key=node_centrality.get, reverse=True)
    n = len(ll)
    ct = 0
    for i in nodes_sorted:
        ct += 1
        for j in ll[i-1]:
            lw[i-1][ll[i-1].index(j)] = weight
            lw[j-1][ll[j-1].index(i)] = weight
        if ct/n > target_perc:
            break
    return lw

def create_list_of_weights_degree_dist(ll, graph, target_perc, weight=.1, descending=True):
    lw = [[1.0]*len(list_) for list_ in ll]
    nodes_sorted = sorted(graph.nodes, key=graph.degree, reverse=descending)
    m = graph.number_of_edges()
    ct = 0
    for i in nodes_sorted:
        for j in graph.neighbors(i):
            if lw[i-1][ll[i-1].index(j)] == weight:
                continue
            else:
                lw[i-1][ll[i-1].index(j)] = weight
                lw[j-1][ll[j-1].index(i)] = weight
                ct += 1
            if ct/m > target_perc:
                break
        if ct/m > target_perc:
            break
    return lw

def create_list_of_weights_degree_dist_target_nodes(ll, graph, target_perc, weight=.1, descending=True):
    lw = [[1.0]*len(list_) for list_ in ll]
    nodes_sorted = sorted(graph.nodes, key=graph.degree, reverse=descending)
    n = graph.number_of_nodes()
    ct = 0
    for i in nodes_sorted:
        ct += 1
        for j in graph.neighbors(i):
            lw[i-1][ll[i-1].index(j)] = weight
            lw[j-1][ll[j-1].index(i)] = weight
        if ct/n > target_perc:
            break
    return lw

def create_list_of_weights_target_random_nodes(ll, graph, target_perc, weight=.1):
    lw = [[1.0]*len(list_) for list_ in ll]
    n = graph.number_of_nodes()
    selected_nodes = sample(graph.nodes(), int(np.ceil(n*target_perc)))
    for i in selected_nodes:
        for j in graph.neighbors(i):
            lw[i-1][ll[i-1].index(j)] = weight
            lw[j-1][ll[j-1].index(i)] = weight
    return lw

def get_data_for_plotting(sol):
    sum_ = [arr.sum(axis=0) for arr in sol]
    sum_s = np.array([arr[0] for arr in sum_])
    sum_e = np.array([arr[1] for arr in sum_])
    sum_i = np.array([arr[2] for arr in sum_])
    sum_r = np.array([arr[3] for arr in sum_])
    return sum_s, sum_e, sum_i, sum_r

def get_max_active_cases(sol):
    sum_ = [arr.sum(axis=0) for arr in sol]
    sum_e = np.array([arr[1] for arr in sum_])
    sum_i = np.array([arr[2] for arr in sum_])
    return max(sum_e+sum_i)

def get_total_active_cases(sol):
    sum_ = [arr.sum(axis=0) for arr in sol]
    sum_r = np.array([arr[3] for arr in sum_])
    return max(sum_r)

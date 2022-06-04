import json
import networkx as nx

# Creates the list of nodes in the graph and the directed edges between them
def generateNodeAndEdgeList(computers):

    # Dictionary storing the mappings between ids and computer names.
    idToComputerName = {}
    computerNameToId = {}
    nodes = []

    id = 1

    # Goes through each computer and gives it an integer for an id (starting from 1) and stores the mapping in id to computer and vice versa.
    for computer in computers:
        nodes.append(id)
        computerNameToId[computer] = id
        idToComputerName[id] = computer
        id+=1

    edges = []

    # Goes through the adjacency list and creates the edge list with the ids we created above.
    for source_computer in computers:
        source_id = computerNameToId[source_computer]
        for dest_computer in computers[source_computer]:
            dest_id = computerNameToId[dest_computer]
            edges.append((source_id, dest_id))

    return nodes, edges

# Saves the networkX graph to file.
def saveGraphToFile(G, path):

    nx.write_graphml(G, path)

# Creates a directed networkX graph and returns it.
def generateGraph(computers, path):

    G = nx.DiGraph()

    nodes, edges = generateNodeAndEdgeList(computers)

    G.add_nodes_from(nodes)
    G.add_edges_from(edges)

    saveGraphToFile(G, path)

# Opens the json file storing the graph data.
f = open('../data/graph4.json')
computers = json.load(f)

# Expects an adjacency list (dictionary) representing the graph and the path for the graph to be saved to.
generateGraph(computers, "../data/graph4.graphml")

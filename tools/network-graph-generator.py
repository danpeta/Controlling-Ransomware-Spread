from neo4j import GraphDatabase
from concurrent.futures import ThreadPoolExecutor,as_completed,thread
import sys
import os
import queue
import json

FILE_PATH = os.getcwd()

class GraphGenerator():

    # Initialises all info required to connect to and run queries on the database.
    def __init__(self):

        self.url = "bolt://localhost:7687"
        self.username = "neo4j"
        self.password = "adslab"
        self.use_encryption = False
        self.connected = False
        self.driver = None
        self.session = None

        self.workers = 25
        self.max_waves = 3

    # Attempts to connect to the database with information specified in the initialisation of the object.
    def connect(self):
        self.connected = False
        if self.driver is not None:
            self.driver.close()
        try:
            self.driver = GraphDatabase.driver(self.url, auth=(self.username, self.password), encrypted=self.use_encryption)
            self.session = self.driver.session()
            self.connected = True
            print("Database connection successful!")
        except:
            self.connected = False
            print("Database connection failed!")

        return self.connected

    # Gets the object_id and name of each computer in the database and stores them as a tuple in an array.
    def get_all_computers(self):

        print("Collecting all computer nodes from database...")
        result = self.session.run("MATCH (c:Computer) RETURN c.objectid AS objectid, c.name as name")
        computers = []
        
        for record in result:
            computers.append((record["objectid"], record["name"]))

        return computers

    # Takes an objectid and returns all the outgoing neighbours of that node.
    def get_neighbours(self, objectid):

        mysession = self.driver.session()
        query = f"MATCH (c:Base {{objectid: \"{objectid}\"}}) CALL apoc.neighbors.athop(c, \"HasSession>|MemberOf>|AdminTo>\", 1) YIELD node RETURN node.objectid AS objectid, node.name AS name"
        result = mysession.run(query)
        return result

    def create_adjacency_list(self):

        # Keeps track of which computers are adjacent to one another.
        adjacency_list = {}

        # Keeps track of the ids of our computers so that we can only stop our search when we find one of these.
        computerIdSet = set()

        computers = self.get_all_computers()

        computerNum = len(computers)

        print(f"There are {computerNum} computers.")

        # Adds all the computer ids to the set.
        for computer in computers:
            
            computerId = computer[0]
            computerIdSet.add(computerId)

        for i, computer in enumerate(computers):

            q = queue.Queue()
            visited = set()

            sourceComputerId, sourceComputerName = computer

            adjacency_list[sourceComputerName] = []

            # Expects an objectid and name for the computer.
            q.put((sourceComputerId, sourceComputerName))

            while not q.empty():

                curr_object_id, curr_object_name = q.get()

                # If we find a computer add it to the adjacency list, else keep expanding the node.
                # If this is the source computer, ignore it.
                if curr_object_id in computerIdSet and curr_object_id != sourceComputerId:
                    adjacency_list[sourceComputerName].append(curr_object_name)
                else:
                    # Go through each neighbour and if it has not been visited add it to the queue.
                    for neighbour in self.get_neighbours(curr_object_id):
                        new_object_id = neighbour["objectid"]
                        if new_object_id not in visited:
                            new_object_name = neighbour["name"]
                            # Add the id, name and whether it is a computer or not to the queue.
                            q.put((new_object_id, new_object_name))
                            visited.add(new_object_id)

            print(len(adjacency_list[sourceComputerName]))
            print(f"{i+1} out of {computerNum} computers processed.")
                            
        return adjacency_list

graphGenerator = GraphGenerator()
if graphGenerator.connect():
    adjacencyList = graphGenerator.create_adjacency_list()
    with open('data.json', 'w', encoding='utf-8') as f:
        json.dump(adjacencyList, f, ensure_ascii=False, indent=4)
else:
    print("Connection to database failed.")

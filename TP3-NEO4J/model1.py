# import the neo4j driver for Python
from neo4j import GraphDatabase

 
# Database Credentials
uri = "bolt://localhost:7687"
username = "neo4j"
password = "1212121212B"

# Connect to the neo4j database server

graphDB_Driver  = GraphDatabase.driver(uri, auth=(username, password))


# CQL to query all the universities present in the graph

cqlNodeQuery    = "MATCH (x:university) RETURN x"


# CQL to delete all existing nodes and relationships
cqlDeleteAll = """MATCH (n) DETACH DELETE n"""


# CQL to query the distances from Yale to some of the other Ivy League universities

cqlEdgeQuery = "MATCH (x:university {name:'Yale University'})-[r]->(y:university) RETURN y.name,r.miles"


# CQL to create a graph containing some of the Ivy League universities

cqlCreate = """CREATE (cornell:university { name: "Cornell University"}),

(yale:university { name: "Yale University"}),

(princeton:university { name: "Princeton University"}),

(harvard:university { name: "Harvard University"}),

 

(cornell)-[:connects_in {miles: 259}]->(yale),

(cornell)-[:connects_in {miles: 210}]->(princeton),

(cornell)-[:connects_in {miles: 327}]->(harvard),

 

(yale)-[:connects_in {miles: 259}]->(cornell),

(yale)-[:connects_in {miles: 133}]->(princeton),

(yale)-[:connects_in {miles: 133}]->(harvard),

 

(harvard)-[:connects_in {miles: 327}]->(cornell),

(harvard)-[:connects_in {miles: 133}]->(yale),

(harvard)-[:connects_in {miles: 260}]->(princeton),

 

(princeton)-[:connects_in {miles: 210}]->(cornell),

(princeton)-[:connects_in {miles: 133}]->(yale),

(princeton)-[:connects_in {miles: 260}]->(harvard)"""

 

# Connect to the Neo4j database server
graphDB_Driver = GraphDatabase.driver(uri, auth=(username, password))

# Function to delete all nodes and relationships from the graph
def delete_all(tx):
    tx.run("MATCH (n) DETACH DELETE n")

# Function to get all nodes from the graph
def get_all_nodes(tx):
    result = tx.run("MATCH (n) RETURN n")
    return [record["n"] for record in result]

# Function to get a specific node by its ID
def get_node_by_id(tx, node_id):
    result = tx.run("MATCH (n) WHERE id(n) = $node_id RETURN n", node_id=node_id)
    record = result.single()
    if record:
        return record["n"]
    else:
        print("Node with ID {} not found.".format(node_id))
        return None


# Function to get all relationships from the graph
def get_all_relationships(tx):
    result = tx.run("MATCH ()-[r]->() RETURN r")
    return [record["r"] for record in result]

# Function to get a specific relationship by its ID
def get_relationship_by_id(tx, relationship_id):
    result = tx.run("MATCH ()-[r]->() WHERE id(r) = $relationship_id RETURN r", relationship_id=relationship_id)
    record = result.single()
    if record:
        return record["r"]
    else:
        print("Failed to find relationship with ID", relationship_id)
        return None

# Function to create a node with given properties
def create_node(tx, properties):
    result = tx.run("CREATE (n) SET n = $properties RETURN n", properties=properties)
    return result.single()["n"]

# Define the function to create a relationship
def create_relationship(tx, start_node_id, end_node_id, rel_type, properties=None):
    query = """
    MATCH (start), (end)
    WHERE id(start) = $start_node_id AND id(end) = $end_node_id
    CREATE (start)-[r:{}]->(end)
    """.format(rel_type)
    if properties:
        query += " SET r = $properties"
    result = tx.run(query, start_node_id=start_node_id, end_node_id=end_node_id, properties=properties)
    if result.single():
        return result.single()["r"]
    else:
        print("Failed to create relationship.")
        return None

# Example usage
with graphDB_Driver.session() as session:

    # Delete nodes
    session.run(cqlDeleteAll)

    # Create nodes
    session.run(cqlCreate)

    # Query the graph    

    nodes = session.run(cqlNodeQuery)

    print("List of Ivy League universities present in the graph:")

    for node in nodes:

        print(node) 

    # Query the relationships present in the graph

    nodes = session.run(cqlEdgeQuery)

    print("Distance from Yale University to the other Ivy League universities present in the graph:")

    for node in nodes:

        print(node)

    print("\n-----------------------------\n")
    # Delete all nodes and relationships
    #session.write_transaction(delete_all)
    print("All nodes and relationships deleted.")
    print("\n-----------------------------\n")
    # Get all nodes
    nodes = session.read_transaction(get_all_nodes)
    print("All Nodes:", nodes)
    print("\n-----------------------------\n")
    # Get node by ID
    node = session.read_transaction(get_node_by_id, node_id=15111)
    print("Node with ID 15111:", node)
    print("\n-----------------------------\n")
    # Get all relationships
    relationships = session.read_transaction(get_all_relationships)
    print("All Relationships:", relationships)
    print("\n-----------------------------\n")
    # Get relationship by ID
    relationship = session.read_transaction(get_relationship_by_id, relationship_id=157732)
    if relationship:
        print("Relationship with ID 157732:", relationship)
    print("\n-----------------------------\n")
    # Create a node
    new_node = session.write_transaction(create_node, properties={"name": "New Node"})
    print("New Node:", new_node)
    print("\n-----------------------------\n")
    # Call the function to create a relationship
    new_relationship = session.write_transaction(create_relationship, start_node_id=15111, end_node_id=15112, rel_type="KNOWS", properties={"since": "2022"})
    print("New Relationship Created")
    print("\n-----------------------------\n")

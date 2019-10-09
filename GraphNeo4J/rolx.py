# so I have found the following code that in theory will convert the Neo4j graph into networkx 
# the graph role algorithm found uses networkx nx graph to run its feature extractor and 
# then its role extractors. I then uses the nodes as defined in networkx to add an
# assigned rol to each one

# all of the code in this python script is from the following post
# http://www.solasistim.net/posts/neo4j_to_networkx/

# this is a sample cypher query that will graph all relationships, currently
# I nee to think about having a way to have relations between nodes, users?
# subgraphs might also be a way to go since grabbing all that data into nerworkx format wil be expensive

# MATCH (n) OPTIONAL MATCH (n)-[r]->() RETURN n, r

# a function that is used to then graph the data from query into network

import networkx
from GraphNeo4J import testneo4j as ts



def rs2graph(rs):
    graph = networkx.MultiDiGraph()

    for record in rs:
        node = record['n']
        if node:
            print("adding node")
            nx_properties = {}
            nx_properties.update(node.properties)
            nx_properties['labels'] = node.labels
            graph.add_node(node.id, **nx_properties)

        relationship = record['r']
        if relationship is not None:   # essential because relationships use hash val
            print("adding edge")
            graph.add_edge(
                relationship.start, relationship.end, key=relationship.type,
                **relationship.properties
            )

    return graph

# using aggregation

# this version expects a collection of rels in the variable 'rels'
# But, this version doesn't handle dangling references


def rs2graph_v2(rs):
    graph = networkx.MultiDiGraph()

    for record in rs:
        node = record['n2']
        if not node:
            raise Exception('every row should have a node')

        print("adding node")
        nx_properties = {}
        nx_properties.update(node.properties)
        nx_properties['labels'] = list(node.labels)
        graph.add_node(node.id, **nx_properties)

        relationship_list = record['rels']

        for relationship in relationship_list:
            print("adding edge")
            graph.add_edge(
                relationship.start, relationship.end, key=relationship.type,
                **relationship.properties
            )

    return graph

# trying to extend to handle subgraphs


# MATCH (a:Token {content: "nonesuch"})-[:PRECEDES*]->(t:Token)
# WITH COLLECT(a) + COLLECT(DISTINCT t) AS nodes_
# UNWIND nodes_ AS n
# OPTIONAL MATCH p = (n)-[r]-()
# WITH n AS n2, COLLECT(DISTINCT RELATIONSHIPS(p)) AS nestedrel
# RETURN n2, REDUCE(output = [], rel in nestedrel | output + rel) AS rels


# This version has to materialize the entire node set up front in order
# to check for dangling references.  This may induce memory problems in large
# result sets
def rs2graph_v3(rs):
    graph = networkx.MultiDiGraph()

    materialized_result_set = list(rs)
    node_id_set = set([
        record['n2'].id for record in materialized_result_set
    ])

    for record in materialized_result_set:
        node = record['n2']
        if not node:
            raise Exception('every row should have a node')

        print("adding node")
        nx_properties = {}
        nx_properties.update(node.properties)
        nx_properties['labels'] = list(node.labels)
        graph.add_node(node.id, **nx_properties)

        relationship_list = record['rels']

        for relationship in relationship_list:
            print("adding edge")

            # Bear in mind that when we ask for all relationships on a node,
            # we may find a node that PRECEDES the current node -- i.e. a node
            # whose relationship starts outside the current subgraph returned
            # by this query.
            if relationship.start in node_id_set:
                graph.add_edge(
                    relationship.start, relationship.end, key=relationship.type,
                    **relationship.properties
                )
            else:
                print("ignoring dangling relationship [no need to worry]")

    return graph


# this code is what I'm hoping will run and be enough to then get the roles out.

uri = "bolt://localhost:7687"

user = "neo4j"
password = "12345"

session = ts.HPCUserDatabase(uri, user, password)
rs = session.query_full_set()
print(rs)
# networkx_graph = rs2graph(rs)

#print(networkx_graph)



session.close() 





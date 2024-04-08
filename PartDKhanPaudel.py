import configparser
from connection import Neo4jConnection  # Ensure this matches the name of your connection file
import os


def load_config(config_file='config.ini'):
    # Try to read from environment variables first
    uri = os.getenv('NEO4J_URI')
    user = os.getenv('NEO4J_USER')
    password = os.getenv('NEO4J_PASSWORD')
    database = os.getenv('NEO4J_DATABASE')

    if uri and user and password and database:
        return {'uri': uri, 'user': user, 'password': password, 'database': database}

    # Fallback to config file if environment variables are not set
    config = configparser.ConfigParser()
    config.read(config_file)
    return config['neo4j']

class GraphAlgorithms:
    def __init__(self, config):
        self.conn = Neo4jConnection(config['uri'], config['user'], config['password'], config['database'])

    def close(self):
        self.conn.close()

    def project_graph(self, graph_name, node_label, relationship_type, orientation='NATURAL'):
        query = f"""
        CALL gds.graph.project(
            '{graph_name}',
            '{node_label}',
            {{
                {relationship_type}: {{
                    type: '{relationship_type}',
                    orientation: '{orientation}'
                }}
            }}
        )
        """
        self.conn.query(query)
        print(f"Graph '{graph_name}' projected successfully.")

    def run_pagerank(self, graph_name):
        query = f"""
        CALL gds.pageRank.stream('{graph_name}')
        YIELD nodeId, score
        RETURN gds.util.asNode(nodeId).paperID AS paperID, score
        ORDER BY score DESC
        LIMIT 10
        """
        results = self.conn.query(query)
        print("PageRank scores:")
        for result in results:
            print(result["paperID"], result["score"])

    # Updated run_betweenness method
    def run_betweenness(self, graph_name):
        query = f"""
        CALL gds.betweenness.stream('{graph_name}')
        YIELD nodeId, score
        RETURN gds.util.asNode(nodeId).paperID AS paperID, score
        ORDER BY score DESC
        LIMIT 10
        """
        results = self.conn.query(query)
        print("Betweenness centrality scores:")
        for result in results:
            print(result["paperID"], result["score"])

    # Updated run_closeness method
    def run_closeness(self, graph_name):
        query = f"""
        CALL gds.closeness.stream('{graph_name}')
        YIELD nodeId, score
        RETURN gds.util.asNode(nodeId).paperID AS paperID, score AS centrality
        ORDER BY centrality DESC
        LIMIT 10
        """
        results = self.conn.query(query)
        print("Closeness centrality scores:")
        for result in results:
            print(result["paperID"], result["centrality"])

    def run_community_detection(self, graph_name):

        # Triangle counting
        triangle_counting_query = f"""
        CALL gds.triangleCount.stream('{graph_name}')
        YIELD nodeId, triangleCount
        RETURN gds.util.asNode(nodeId).paperID AS paperID, triangleCount
        ORDER BY triangleCount DESC
        LIMIT 10;
        """
        triangle_count_results = self.conn.query(triangle_counting_query)
        print("Triangle counting detection:")
        for result in triangle_count_results:
            print(result["paperID"], result["triangleCount"])


        # Louvain
        louvain_query = f"""
        CALL gds.louvain.stream('{graph_name}')
        YIELD nodeId, communityId
        RETURN gds.util.asNode(nodeId).paperID AS paperID, communityId
        ORDER BY communityId DESC limit 10
        """
        louvain_results = self.conn.query(louvain_query)
        print("Louvain community detection:")
        for result in louvain_results:
            print(result["paperID"], result["communityId"])

        # Add similarly for SCC and WCC
        # Strongly Connected Components (SCC)
        result = self.conn.query(f"""
                CALL gds.scc.stream('{graph_name}')
                YIELD nodeId, componentId
                RETURN gds.util.asNode(nodeId).paperID AS paperID, componentId
                ORDER BY componentId DESC limit 10
                """)
        print("Strongly Connected Components:")
        for record in result:
            print(record["paperID"], record["componentId"])

        # Weakly Connected Components (WCC)
        result = self.conn.query(f"""
                    CALL gds.wcc.stream('{graph_name}')
                    YIELD nodeId, componentId
                    RETURN gds.util.asNode(nodeId).paperID AS paperID, componentId
                    ORDER BY componentId DESC limit 10
                    """)
        print("Weakly Connected Components:")
        for record in result:
            print(record["paperID"], record["componentId"])

if __name__ == "__main__":
    config = load_config()
    graph_algo = GraphAlgorithms(config)

    graph_algo.project_graph("paper_cites", "Paper", "CITES")
    graph_algo.project_graph("paper_cites_undirected", "Paper", "CITES", orientation="UNDIRECTED")

    graph_algo.run_pagerank("paper_cites")
    graph_algo.run_betweenness("paper_cites")
    graph_algo.run_closeness("paper_cites")
    graph_algo.run_community_detection("paper_cites_undirected")

    graph_algo.close()

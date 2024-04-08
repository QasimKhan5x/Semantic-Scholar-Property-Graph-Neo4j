import pandas as pd
from connection import Neo4jConnection
import configparser
import os

# Function to load configuration
def load_config(config_file='config.ini'):
    # Try to read from environment variables first
    uri = os.getenv('NEO4J_URI')
    user = os.getenv('NEO4J_USER')
    password = os.getenv('NEO4J_PASSWORD')
    db = os.getenv('NEO4J_DATABASE')

    if uri and user and password and db:
        return {'uri': uri, 'user': user, 'password': password, 'database': db}, 

    # Fallback to config file if environment variables are not set
    config = configparser.ConfigParser()
    config.read(config_file)
    return config['neo4j']

# Function to run a query and return a DataFrame
def run_query(conn, query):
    query_result = conn.query(query)
    return pd.DataFrame([dict(record) for record in query_result])


# Define functions for each query
def get_top3_papers_per_conference(conn):
    query = """MATCH (c:Conference)<-[:PRESENTED_IN]-(p:Proceedings {type: "conference"})<-[:PUBLISHED_IN]-(paper:Paper)
    OPTIONAL MATCH (paper)<-[:CITES]-(citingPaper:Paper)
    WITH c, paper, COUNT(citingPaper) AS citations
    ORDER BY citations DESC
    WITH c.name AS ConferenceName, collect({title: paper.title, citations: citations}) AS papers
    RETURN ConferenceName, [paper IN papers[0..3] | paper.title] AS Top3Papers, [paper IN papers[0..3] | paper.citations] AS Citations
    ORDER BY ConferenceName"""
    return run_query(conn, query)


def get_conference_community(conn):
    query = """MATCH (a:Author)-[:WRITES]->(p:Paper)-[:PUBLISHED_IN]->(pro:Proceedings {type: "conference"})-[:PRESENTED_IN]->(c:Conference)
    WITH c.name AS ConferenceName, a, COUNT(DISTINCT pro.edition) AS EditionsPublished
    WHERE EditionsPublished >= 4
    RETURN ConferenceName, COLLECT(a.name) AS Community
    ORDER BY ConferenceName"""
    return run_query(conn, query)


def get_impact_factors(conn):
    query = """MATCH (j:Journal)<-[:PRESENTED_IN]-(v:Volume)<-[:PUBLISHED_IN]-(p:Paper)-[:IN_YEAR]->(y:Year)
    WHERE y.year IN [2019, 2020]
    WITH j, p
    OPTIONAL MATCH (cp:Paper)-[:CITES]->(p)
    WITH j, p, COLLECT(cp) AS citedPapers
    MATCH (citedPaper:Paper)-[:IN_YEAR]->(cy:Year)
    WHERE citedPaper IN citedPapers AND cy.year = 2021
    WITH j, p, COUNT(citedPaper) AS citations
    WITH j, SUM(citations) AS totalCitations, COUNT(DISTINCT p) AS totalPapers
    RETURN j.name AS Journal, totalCitations, totalPapers, CASE WHEN totalPapers > 0 THEN totalCitations * 1.0 / totalPapers ELSE 0 END AS ImpactFactor
    ORDER BY ImpactFactor DESC"""
    return run_query(conn, query)


def get_h_indexes(conn):
    query = """MATCH (a:Author)-[:WRITES]->(p:Paper)
    OPTIONAL MATCH (p)<-[:CITES]-(citing:Paper)
    WITH a, p, COUNT(citing) AS citations
    ORDER BY citations DESC
    WITH a, COLLECT(citations) AS citationCounts
    UNWIND RANGE(1, SIZE(citationCounts)) AS index
    WITH a, citationCounts, index
    WHERE citationCounts[index-1] >= index
    WITH a, MAX(index) AS hIndex
    RETURN a.name AS Author, hIndex AS HIndex
    ORDER BY HIndex DESC"""
    return run_query(conn, query)


def main():
    config = load_config()


    # Connect to Neo4j
    conn = Neo4jConnection(config['uri'], config['user'], config['password'], config['database'])

    try:
        # Execute queries
        top3_papers_df = get_top3_papers_per_conference(conn)
        community_df = get_conference_community(conn)
        impact_factors_df = get_impact_factors(conn)
        h_indexes_df = get_h_indexes(conn)

        # Save to CSV or process as required
        top3_papers_df.to_csv('top3_papers_per_conference.csv', index=False)
        community_df.to_csv('conference_community.csv', index=False)
        impact_factors_df.to_csv('impact_factors.csv', index=False)
        h_indexes_df.to_csv('h_indexes.csv', index=False)

        # You could also output to console
        print("Top 3 Papers Per Conference:")
        print(top3_papers_df)
        print("\nConference Community:")
        print(community_df)
        print("\nJournal Impact Factors:")
        print(impact_factors_df)
        print("\nAuthor H-Indexes:")
        print(h_indexes_df)

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        conn.close()
        print("Connection to Neo4j closed.")


if __name__ == "__main__":
    main()

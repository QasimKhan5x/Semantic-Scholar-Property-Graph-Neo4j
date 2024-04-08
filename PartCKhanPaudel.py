import pandas as pd
from connection import Neo4jConnection
import configparser
import os


def load_config(config_file="config.ini"):
    # Try to read from environment variables first
    uri = os.getenv("NEO4J_URI")
    user = os.getenv("NEO4J_USER")
    password = os.getenv("NEO4J_PASSWORD")
    database = os.getenv("NEO4J_DATABASE")

    print(uri, user, password, database)

    if uri and user and password:
        return {"uri": uri, "user": user, "password": password, "database": database}

    # Fallback to config file if environment variables are not set
    config = configparser.ConfigParser()
    config.read(config_file)
    return config["neo4j"]


# Function to run a query and return a DataFrame
def run_query(conn, query):
    query_result = conn.query(query)
    return pd.DataFrame([dict(record) for record in query_result])


def create_database_community(conn):
    create_db_comm = "CREATE (:Community {name: 'Graph'});"
    conn.query(create_db_comm)


def associate_keywords_with_community(conn):
    associate_keywords = """UNWIND ['graph', 'graph neural', 'knowledge graphs', 'knowledge graph', 'bipartite graphs', 'graph convolutional'] AS keywordName
    MERGE (k:Keyword {keyword: keywordName})
    WITH k
    MATCH (c:Community {name: 'Graph'})
    MERGE (k)-[:PART_OF]->(c);"""
    conn.query(associate_keywords)


def tag_conferences_and_journals(conn):
    tag_conference = """MATCH (p:Paper)-[:CONTAINS]->(k:Keyword)-[:PART_OF]->(c:Community {name: 'Graph'})
    WITH p, COUNT(k) AS relevance
    MATCH (p)-[:PUBLISHED_IN]->()-[:PRESENTED_IN]->(con)
    WITH con, COLLECT(p) AS papers, COUNT(p) AS totalPapers, SUM(relevance) AS totalRelevance
    WHERE totalRelevance / totalPapers >= 0.9
    SET con:GraphSpecific;"""
    conn.query(tag_conference)

    tag_journal = """MATCH (p:Paper)-[:CONTAINS]->(k:Keyword)-[:PART_OF]->(c:Community {name: 'Graph'})
    MATCH (p)-[:PUBLISHED_IN]->(v:Volume)-[:PRESENTED_IN]->(j:Journal)
    WITH j, v, COUNT(DISTINCT p) AS relatedPapers, COLLECT(DISTINCT p) AS papers
    MATCH (v)-[:PRESENTED_IN]->(j)
    WITH j, SUM(relatedPapers) AS totalRelatedPapers, COLLECT(papers) AS allPapers, COUNT(DISTINCT v) AS totalVolumes
    WHERE totalRelatedPapers / totalVolumes >= 0.9
    SET j:GraphSpecific
    RETURN j.name, totalRelatedPapers, totalVolumes;"""
    return run_query(conn, tag_journal)


def identify_top_cited_papers(conn):
    top_cited = """MATCH (p1:Paper)-[:PUBLISHED_IN]->()-[:PRESENTED_IN]->(con:GraphSpecific), (p2:Paper)-[:CITES]->(p1)
    WHERE EXISTS((p2)-[:PUBLISHED_IN]->()-[:PRESENTED_IN]->(:GraphSpecific))
    WITH p1, COUNT(p2) AS citations
    ORDER BY citations DESC
    LIMIT 100
    SET p1:Top100
    RETURN p1.title AS TopPapers, citations
    ORDER BY citations DESC;
    """
    return run_query(conn, top_cited)


def find_potential_reviewers_and_gurus(conn):
    top_reviewers = """MATCH (a:Author)-[:WRITES]->(p:Paper)
    WHERE p:Top100
    MERGE (a)-[:POTENTIAL_REVIEWER_FOR]->(Community{name:'Graph'});
    """
    conn.query(top_reviewers)

    find_gurus = """MATCH (a:Author)-[:WRITES]->(p:Paper)
    WHERE p:Top100
    WITH a, COUNT(p) AS papers
    WHERE papers >= 2
    MERGE (a)-[:GURU_FOR]->(Community{name:'Graph'})
    RETURN a.name AS AuthorName, papers AS NumberOfTopPapers;
    """
    return run_query(conn, find_gurus)


def main():
    config = load_config()

    conn = Neo4jConnection(
        config["uri"], config["user"], config["password"], config["database"]
    )

    try:
        # Execute the steps for the recommender system
        create_database_community(conn)
        associate_keywords_with_community(conn)
        tagged_journals_df = tag_conferences_and_journals(conn)
        top_cited_papers_df = identify_top_cited_papers(conn)
        gurus_df = find_potential_reviewers_and_gurus(conn)

        # Save the output or process as required
        tagged_journals_df.to_csv("tagged_journals.csv", index=False)
        top_cited_papers_df.to_csv("top_cited_papers.csv", index=False)
        gurus_df.to_csv("gurus.csv", index=False)

        # You could also output to console
        print("Tagged Journals:")
        print(tagged_journals_df)
        print("\nTop Cited Papers:")
        print(top_cited_papers_df)
        print("\nGurus:")
        print(gurus_df)

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        conn.close()
        print("Connection to Neo4j closed.")


if __name__ == "__main__":
    main()

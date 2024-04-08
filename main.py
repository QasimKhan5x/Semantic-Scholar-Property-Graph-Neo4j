import argparse
import configparser
import os
from neo4j import GraphDatabase


# Your Neo4jConnection class here
class Neo4jConnection:
    def __init__(self, uri, user, password):
        self.__uri = uri
        self.__user = user
        self.__password = password
        self.__driver = None
        try:
            self.__driver = GraphDatabase.driver(self.__uri, auth=(self.__user, self.__password))
        except Exception as e:
            print("Failed to create the driver:", e)

    def close(self):
        if self.__driver is not None:
            self.__driver.close()

    def query(self, query, parameters=None, db=None):
        session = None
        response = None
        try:
            session = self.__driver.session(database=db) if db is not None else self.__driver.session()
            response = list(session.run(query, parameters))
        except Exception as e:
            print("Query failed:", e)
        finally:
            if session is not None:
                session.close()
        return response

def load_config(config_file):
    config = configparser.ConfigParser()
    config.read(config_file)
    return config


def load_authors(conn, csv_path):
    query = f"""
    LOAD CSV WITH HEADERS FROM '{csv_path}' AS row
    MERGE (author:Author {{authorId: row.authorId, name: row.name}});
    """
    conn.query(query)
    print("Authors loaded successfully.")

def load_papers(conn, csv_path):
    query = f"""
    LOAD CSV WITH HEADERS FROM '{csv_path}' AS row
    CREATE (p:Paper {{
        paperId: row.paperId,
        title: row.title,
        abstract: row.abstract,
        publicationDate: row.publicationDate
    }})
    """
    conn.query(query)
    print("Papers loaded successfully.")

def load_author_paper_relationships(conn, csv_path):
    query = f"""
    LOAD CSV WITH HEADERS FROM '{csv_path}' AS line
    MATCH (a:Author {{authorId: line.authorId}}), (p:Paper {{paperId: line.paperId}})
    MERGE (a)-[:WRITE]->(p)
    """
    conn.query(query)
    print("Author-paper relationships loaded successfully.")

def update_write_relationships(conn, csv_path):
    query = f"""
    LOAD CSV WITH HEADERS FROM '{csv_path}' AS row
    MATCH (a:Author {{authorId: row.authorId}})-[r:WRITE]->(p:Paper {{paperId: row.paperId}})
    SET r.isCorrespondence = CASE WHEN TRIM(TOUPPER(row.isCorrespondence)) = 'TRUE' THEN true ELSE false END
    """
    conn.query(query)
    print("Write relationships updated successfully.")

def load_volumes(conn, csv_path):
    query = f"""
    LOAD CSV WITH HEADERS FROM '{csv_path}' AS row
    MERGE (v:Volume {{
        volumeID: row.volumeID,
        year: toInteger(row.year),
        quarter: toInteger(row.quarter)
    }})
    """
    conn.query(query)
    print("Volumes loaded successfully.")

def load_paper_volume_relationships(conn, csv_path):
    query = f"""
    LOAD CSV WITH HEADERS FROM '{csv_path}' AS row
    MERGE (v:Volume {{volumeID: row.volumeID}})
    ON CREATE SET v.year = toInteger(row.year), v.quarter = toInteger(row.quarter)
    WITH v, row
    MERGE (p:Paper {{paperId: row.paperId}})
    MERGE (p)-[:INCLUDED_IN]->(v)
    """
    conn.query(query)
    print("Paper-volume relationships loaded successfully.")

def load_journals(conn, csv_path):
    query = f"""
    LOAD CSV WITH HEADERS FROM '{csv_path}' AS line
    CREATE (:Journal {{journalID: line.journalID, name: line.name}});
    """
    conn.query(query)
    print("Journals loaded successfully.")

def load_journal_volume_relationships(conn, csv_path):
    query = f"""
    LOAD CSV WITH HEADERS FROM '{csv_path}' AS line
    MATCH (journal:Journal {{journalID: line.journalID}})
    MATCH (volume:Volume {{volumeID: line.volumeID}})
    MERGE (volume)-[:PUBLISHED_IN]->(journal);
    """
    conn.query(query)
    print("Journal-volume relationships loaded successfully.")


def load_proceedings(conn, csv_path):
    query = f"""
    LOAD CSV WITH HEADERS FROM '{csv_path}' AS row
    CREATE (proc:Proceedings {{
        proceedingsId: row.proceedingsId,
        venue: row.venue,
        startDate: row.start_date,
        endDate: row.end_date,
        edition: toInteger(row.edition),
        type: row.type
    }});
    """
    conn.query(query)
    print("Proceedings loaded successfully.")

def load_paper_proceedings_relationships(conn, csv_path):
    query = f"""
    LOAD CSV WITH HEADERS FROM '{csv_path}' AS row
    MATCH (paper:Paper {{paperId: row.paperId}})
    MATCH (proc:Proceedings {{proceedingsId: row.proceedingsId}})
    MERGE (paper)-[:PUBLISHED_IN]->(proc);
    """
    conn.query(query)
    print("Paper-proceedings relationships loaded successfully.")

def load_conferences(conn, csv_path):
    query = f"""
    LOAD CSV WITH HEADERS FROM '{csv_path}' AS row
    CREATE (c:Conference {{
        conferenceId: row.conferenceId,
        conferenceName: row.conferenceName,
        chairName: row.chairName
    }});
    """
    conn.query(query)
    print("Conferences loaded successfully.")

def load_proceedings_conference_relationships(conn, csv_path):
    query = f"""
    LOAD CSV WITH HEADERS FROM '{csv_path}' AS row
    MATCH (c:Conference {{conferenceId: row.conferenceId}})
    MATCH (p:Proceedings {{proceedingsId: row.proceedingsId}})
    MERGE (p)-[:PRESENTED_IN]->(c);
    """
    conn.query(query)
    print("Proceedings-conference relationships loaded successfully.")

def load_workshops(conn, csv_path):
    query = f"""
    LOAD CSV WITH HEADERS FROM '{csv_path}' AS row
    CREATE (w:Workshop {{
        workshopId: row.workshopId,
        name: row.name,
        ISSN: row.ISSN
    }});
    """
    conn.query(query)
    print("Workshops loaded successfully.")

def load_workshops_proceedings_relationships(conn, csv_path):
    query = f"""
    LOAD CSV WITH HEADERS FROM '{csv_path}' AS row
    MATCH (w:Workshop {{workshopId: row.workshopId}})
    MATCH (p:Proceedings {{proceedingsId: row.proceedingsId}})
    MERGE (w)-[:PRESENTED_IN]->(p);
    """
    conn.query(query)
    print("Workshops-proceedings relationships loaded successfully.")

def load_years(conn, csv_path):
    query = f"""
    LOAD CSV WITH HEADERS FROM '{csv_path}' AS row
    CREATE (y:Year {{ year: toInteger(row.year), yearId: toInteger(row.yearId) }});
    """
    conn.query(query)
    print("Years loaded successfully.")

def load_paper_year_relationships(conn, csv_path):
    query = f"""
    LOAD CSV WITH HEADERS FROM '{csv_path}' AS row
    MATCH (p:Paper {{paperId: row.paperId}})
    MATCH (y:Year {{yearId: toInteger(row.yearId)}})
    MERGE (p)-[:PUBLISHED_ON]->(y);
    """
    conn.query(query)
    print("Paper-year relationships loaded successfully.")

def load_paper_citations(conn, csv_path):
    query = f"""
    LOAD CSV WITH HEADERS FROM '{csv_path}' AS row
    MATCH (paper:Paper {{paperId: row.paperId}})
    MATCH (citedPaper:Paper {{paperId: row.citedPaperId}})
    MERGE (paper)-[:CITES]->(citedPaper);
    """
    conn.query(query)
    print("Paper citations loaded successfully.")

def load_author_reviews(conn, csv_path):
    query = f"""
    LOAD CSV WITH HEADERS FROM '{csv_path}' AS row
    MATCH (a:Author {{authorId: row.reviewerId}}), (p:Paper {{paperId: row.paperId}})
    MERGE (a)-[:REVIEW]->(p);
    """
    conn.query(query)
    print("Author reviews loaded successfully.")

def update_review_details(conn, csv_path):
    query = f"""
    LOAD CSV WITH HEADERS FROM '{csv_path}' AS row
    MATCH (a:Author {{authorId: row.reviewerId}})-[r:REVIEW]->(p:Paper {{paperId: row.paperId}})
    SET r.content = row.content, r.acceptance = row.acceptance;
    """
    conn.query(query)
    print("Review details updated successfully.")

def load_organizations(conn, csv_path):
    query = f"""
    LOAD CSV WITH HEADERS FROM '{csv_path}' AS row
    MERGE (org:Organization {{organizationId: row.organizationId}})
    ON CREATE SET org.type = row.organizationType;
    """
    conn.query(query)
    print("Organizations loaded successfully.")

def load_author_affiliations(conn, csv_path):
    query = f"""
    LOAD CSV WITH HEADERS FROM '{csv_path}' AS row
    MATCH (a:Author {{authorId: row.authorId}})
    MATCH (org:Organization {{organizationId: row.organizationId}})
    MERGE (a)-[:AFFILIATED_WITH]->(org);
    """
    conn.query(query)
    print("Author affiliations loaded successfully.")

def load_keywords(conn, csv_path):
    query = f"""
    LOAD CSV WITH HEADERS FROM '{csv_path}' AS row
    MERGE (k:Keyword {{keywordId: row.KeywordId, name: row.Keywords}});
    """
    conn.query(query)
    print("Keywords loaded successfully.")

def load_paper_keywords_relationships(conn, csv_path):
    query = f"""
    LOAD CSV WITH HEADERS FROM '{csv_path}' AS row
    MATCH (p:Paper {{paperId: row.paperId}})
    MATCH (k:Keyword {{keywordId: row.KeywordId}})
    MERGE (p)-[:CONTAINS]->(k);
    """
    conn.query(query)
    print("Paper-keyword relationships loaded successfully.")



def main():
    parser = argparse.ArgumentParser(description='Load data into Neo4j.')
    parser.add_argument('--config', type=str, default='config.ini', help='Path to configuration file.')
    # Add other arguments as needed
    args = parser.parse_args()

    config = load_config(args.config)

    uri = os.getenv("NEO4J_URI", config.get('neo4j', 'uri'))
    user = os.getenv("NEO4J_USER", config.get('neo4j', 'user'))
    password = os.getenv("NEO4J_PASSWORD", config.get('neo4j', 'password'))

    conn = Neo4jConnection(uri, user, password)

    try:
        authors_csv_path = config.get('csv_paths', 'authors')
        load_authors(conn, authors_csv_path)
        # Load other entities as needed
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        conn.close()
        print("Connection closed.")


if __name__ == "__main__":
    main()

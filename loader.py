import argparse
import configparser
import os
from connection import Neo4jConnection
import PartAKhanPaudel as dlf


def load_config(config_file):
    config = configparser.ConfigParser()
    config.read(config_file)
    return config


def main():
    parser = argparse.ArgumentParser(description='Load data into Neo4j.')
    parser.add_argument('--config', type=str, default='config.ini', help='Path to configuration file.')
    # Add other arguments as needed
    args = parser.parse_args()

    config = load_config(args.config)

    uri = os.getenv("NEO4J_URI", config.get('neo4j', 'uri'))
    user = os.getenv("NEO4J_USER", config.get('neo4j', 'user'))
    password = os.getenv("NEO4J_PASSWORD", config.get('neo4j', 'password'))
    db = os.getenv("NEO4J_DATABASE", config.get('neo4j', 'database'))

    conn = Neo4jConnection(uri, user, password, db)

    # Define a mapping of config keys to loading functions
    load_tasks = {
        'authors': dlf.load_authors,
        'years': dlf.load_years,
        'papers': dlf.load_papers,
        'journals': dlf.load_journals,
        'volumes': dlf.load_volumes,
        'paper_volume': dlf.load_paper_volume_relationships,
        'conferences': dlf.load_conferences,
        'workshops': dlf.load_workshops,
        'proceedings': dlf.load_proceedings,
        'writes': dlf.load_writes,
        'reviews': dlf.load_reviews,
        'cites': dlf.load_cites,
        'paper_proceedings': dlf.load_paper_proceedings_relationships,
        'keywords': dlf.load_keywords,
        'paper_keywords': dlf.load_paper_keywords_relationships,
    }
    evolve_tasks = {
        "reviews": dlf.update_review_details,
        "journals": dlf.update_journal_reviewer_policy,
        "conferences": dlf.update_conference_reviewer_policy,
        "workshops": dlf.update_workshop_reviewer_policy,
        "organizations": dlf.load_organizations,
        "authors": dlf.load_author_affiliations
    }

    try:
        print("Loading data into Neo4j...")
        for key, func in load_tasks.items():
                csv_path = config.get('csv_paths', key)
                func(conn, csv_path)
        print("Evolving graph schema...")
        for key, func in evolve_tasks.items():
            csv_path = config.get('csv_paths', key)
            func(conn, csv_path)
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        conn.close()
        print("Connection closed.")

if __name__ == "__main__":
    main()

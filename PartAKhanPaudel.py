def load_authors(conn, csv_path):
    query = f"""LOAD CSV WITH HEADERS FROM '{csv_path}' AS row
    CREATE (:Author {{authorID: row.authorID, name: row.name}});"""
    conn.query(query)
    print("Authors loaded successfully.")


def load_years(conn, csv_path):
    query = f"""LOAD CSV WITH HEADERS FROM '{csv_path}' AS row
    CREATE (:Year {{
    year: toInteger(row.year)
    }});"""
    conn.query(query)
    print("Years loaded successfully.")


def load_papers(conn, csv_path):
    query = f"""LOAD CSV WITH HEADERS FROM '{csv_path}' AS row
    MERGE (p:Paper {{
    paperID: row.paperID,
    title: row.title,
    abstract: row.abstract,
    publicationDate: date(row.publicationDate)
    }})
    MERGE (y:Year {{year: toInteger(row.year)}})
    MERGE (p)-[:IN_YEAR]->(y);
    """
    conn.query(query)
    print("Papers loaded successfully.")


def load_journals(conn, csv_path):
    query = f"""LOAD CSV WITH HEADERS FROM '{csv_path}' AS row
    CREATE (:Journal {{journalID: row.journalID, name: row.name, 
    issn: row.issn, editor: row.editor}});"""
    conn.query(query)
    print("Journals loaded successfully.")


def load_volumes(conn, csv_path):
    query = f"""LOAD CSV WITH HEADERS FROM '{csv_path}' AS row
    CREATE (:Volume {{
    volID: row.volID,
    volNumber: toInteger(row.volNumber),
    journalID: row.journalID
    }});"""
    conn.query(query)
    print("Volumes loaded successfully.")
    query = f"""MATCH (v:Volume), (j:Journal)
    WHERE v.journalID = j.journalID
    CREATE (v)-[:PRESENTED_IN]->(j);"""
    conn.query(query)
    print("Volume PRESENTED_IN Journal relationships loaded successfully.")


def load_paper_volume_relationships(conn, csv_path):
    query = f"""LOAD CSV WITH HEADERS FROM '{csv_path}' AS row
    MATCH (p:Paper {{paperID: row.paperID}})
    MATCH (v:Volume {{volID: row.volID}})
    MERGE (p)-[:PUBLISHED_IN]->(v);"""
    conn.query(query)
    print("Paper PUBLISHED_IN Volume relationship loaded successfully.")


def load_conferences(conn, csv_path):
    query = f"""LOAD CSV WITH HEADERS FROM '{csv_path}' AS row
    CREATE (:Conference {{conferenceID: row.conferenceID, 
    name: row.name, chair: row.chair}});
    """
    conn.query(query)
    print("Conferences loaded successfully.")


def load_workshops(conn, csv_path):
    query = f"""LOAD CSV WITH HEADERS FROM '{csv_path}' AS row
    CREATE (:Workshop {{workshopID: row.workshopID, name: row.name, 
                   chair: row.chair}});"""
    conn.query(query)
    print("Workshops loaded successfully.")


def load_proceedings(conn, csv_path):
    query = f"""LOAD CSV WITH HEADERS FROM '{csv_path}' AS row
    CREATE (:Proceedings {{
    proceedingsID: row.proceedingsID,
    edition: row.edition,
    conferenceID: row.conferenceID,
    type: row.type,
    venue: row.venue,
    startDate: date(row.startDate),
    endDate: date(row.endDate)
    }});"""
    conn.query(query)
    print("Proceedings loaded successfully.")
    query = f"""MATCH (p:Proceedings {{type: "conference"}}), (c:Conference)
    WHERE p.conferenceID = c.conferenceID
    CREATE (p)-[:PRESENTED_IN]->(c)"""
    conn.query(query)
    print("Proceedings PRESENTED_IN Conference relationships loaded successfully.")
    query = f"""MATCH (p:Proceedings {{type: "workshop"}}), (w:Workshop)
    WHERE p.conferenceID = w.conferenceID
    CREATE (p)-[:PRESENTED_IN]->(w)"""
    conn.query(query)
    print("Proceedings PRESENTED_IN Workshop relationships loaded successfully.")


def load_writes(conn, csv_path):
    query = f"""LOAD CSV WITH HEADERS FROM '{csv_path}' AS row
    MATCH (a:Author {{authorID: row.authorID}})
    MATCH (p:Paper {{paperID: row.paperID}})
    MERGE (a)-[r:WRITES]->(p)
    SET r.corresponding = row.corresponds = "True";
    """
    conn.query(query)
    print("Author WRITES Paper relationship loaded successfully.")


def load_reviews(conn, csv_path):
    query = f"""LOAD CSV WITH HEADERS FROM '{csv_path}' AS row
    MATCH (reviewingAuthor:Author {{authorID: row.authorID}})
    MATCH (reviewedPaper:Paper {{paperID: row.paperID}})
    MERGE (reviewingAuthor)-[:REVIEWS]->(reviewedPaper);"""
    conn.query(query)
    print("Author REVIEWS Paper relationship loaded successfully.")


def load_cites(conn, csv_path):
    query = f"""LOAD CSV WITH HEADERS FROM '{csv_path}' AS row
    MATCH (citingPaper:Paper {{paperID: row.paperID}})
    MATCH (citedPaper:Paper {{paperID: row.referenceID}})
    MERGE (citingPaper)-[:CITES]->(citedPaper);"""
    conn.query(query)
    print("Paper CITES Paper relationship loaded successfully.")


def load_paper_proceedings_relationships(conn, csv_path):
    query = f"""LOAD CSV WITH HEADERS FROM '{csv_path}' AS row
    MATCH (p:Paper {{paperID: row.paperID}})
    MATCH (pr:Proceedings {{proceedingsID: row.proceedingsID}})
    MERGE (p)-[:PUBLISHED_IN]->(pr);"""
    conn.query(query)
    print("Paper PUBLISHED_IN Proceedings relationships loaded successfully.")


def load_keywords(conn, csv_path):
    query = f"""LOAD CSV WITH HEADERS FROM '{csv_path}' AS row
    CREATE (:Keyword {{
    keyword: row.keyword
    }});"""
    conn.query(query)
    print("Keywords loaded successfully.")


def load_paper_keywords_relationships(conn, csv_path):
    query = f"""LOAD CSV WITH HEADERS FROM '{csv_path}' AS row WITH row
    WHERE row.keywords IS NOT NULL
    MATCH (p:Paper {{paperID: row.paperId}})
    MERGE (k:Keyword {{keyword: row.keywords}})
    MERGE (p)-[:CONTAINS]->(k);
    """
    conn.query(query)
    print("Paper-keyword relationships loaded successfully.")


def update_review_details(conn, csv_path):
    query = f"""LOAD CSV WITH HEADERS FROM '{csv_path}' AS row
    MATCH (a:Author)-[r:REVIEWS]->(p:Paper)
    WHERE a.authorID = row.authorID AND p.paperID = row.paperID
    SET r.content = row.content, r.decision = (row.decision = "True");"""
    conn.query(query)
    print("Review details updated successfully.")


def update_journal_reviewer_policy(conn, csv_path):
    query = f"""LOAD CSV WITH HEADERS FROM '{csv_path}' AS row
    MATCH (j:Journal)
    WHERE j.journalID = row.journalID
    SET j.reviewerPolicy = row.reviewerPolicy;"""
    conn.query(query)
    print("Journal reviewer policy updated successfully.")


def update_conference_reviewer_policy(conn, csv_path):
    query = f"""LOAD CSV WITH HEADERS FROM '{csv_path}' AS row
    MATCH (c:Conference)
    WHERE c.conferenceID = row.conferenceID
    SET c.reviewerPolicy = row.reviewerPolicy;"""
    conn.query(query)
    print("Conference reviewer policy updated successfully.")


def update_workshop_reviewer_policy(conn, csv_path):
    query = f"""LOAD CSV WITH HEADERS FROM '{csv_path}' AS row
    MATCH (w:Workshop)
    WHERE w.workshopID = row.workshopID
    SET w.reviewerPolicy = row.reviewerPolicy;"""
    conn.query(query)
    print("Workshop reviewer policy updated successfully.")


def load_organizations(conn, csv_path):
    query = f"""LOAD CSV WITH HEADERS FROM '{csv_path}' AS row
    CREATE (:Organization {{orgID: row.orgID, name: row.name, type: row.type}});"""
    conn.query(query)
    print("Organizations loaded successfully.")


def load_author_affiliations(conn, csv_path):
    query = f"""LOAD CSV WITH HEADERS FROM '{csv_path}' AS row
    MATCH (a:Author {{authorID: row.authorID}})
    MATCH (o:Organization {{orgID: row.affiliation}})
    MERGE (a)-[:AFFILIATED_TO]->(o);"""
    conn.query(query)
    print("Author affiliations loaded successfully.")

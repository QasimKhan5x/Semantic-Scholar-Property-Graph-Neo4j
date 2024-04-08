from neo4j import GraphDatabase


class Neo4jConnection:
    def __init__(self, uri, user, password, db=None):
        self.__uri = uri
        self.__user = user
        self.__password = password
        self.__driver = None
        self.__db = db
        try:
            self.__driver = GraphDatabase.driver(self.__uri, auth=(self.__user, self.__password))
        except Exception as e:
            print("Failed to create the driver:", e)

    def close(self):
        if self.__driver is not None:
            self.__driver.close()

    def query(self, query, parameters=None):
        session = None
        response = None
        try:
            session = self.__driver.session(database=self.__db) if self.__db is not None else self.__driver.session()
            response = list(session.run(query, parameters))
        except Exception as e:
            print("Query failed:", e)
        finally:
            if session is not None:
                session.close()
        return response

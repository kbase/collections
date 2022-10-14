import arango


class ArangoStorage:

    def __init__(self, host, database, collection_name, username, password):
        self.arango_db = self.conn_db(host, database, username, password)
        self.collection = self.arango_db.collection(collection_name)

    @staticmethod
    def conn_db(host, database, username, password):
        client = arango.ArangoClient(hosts=host)
        db = client.db(database, username, password, verify=True)

        return db

    def insert_one(self, doc, upsert=False):
        try:
            self.collection.insert(doc, silent=True, overwrite=upsert)
        except arango.exceptions.DocumentInsertError as e:
            raise Exception('Failed to insert doc') from e

    def insert_many(self, docs, upsert=False):
        try:
            self.collection.insert_many(docs, silent=True, overwrite=upsert)
        except arango.exceptions.DocumentInsertError as e:
            raise Exception('Failed to insert doc') from e

    def import_bulk(self, docs, upsert=False):
        try:
            self.collection.import_bulk(docs, overwrite=upsert)
        except arango.exceptions.DocumentInsertError as e:
            raise Exception('Failed to insert doc') from e

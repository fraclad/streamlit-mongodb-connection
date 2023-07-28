from streamlit.connections import ExperimentalBaseConnection
from streamlit.runtime.caching import cache_data
import pymongo
from pymongo import MongoClient
from pymongo.collection import Collection
from typing import List, Union
import certifi

class MongoDBConnection(ExperimentalBaseConnection[MongoClient]):
    """Basic st.experimental_connection implementation for MongoDB"""

    def __init__(self, db_name: str, collection_name: str, **kwargs):
        super().__init__(**kwargs)
        self.db_name = db_name
        self.collection_name = collection_name

    def _connect(self, **kwargs) -> MongoClient:
        if 'database_url' in kwargs:
            database_url = kwargs.pop('database_url')
        else:
            database_url = self._secrets['mongo_connect_url'] 
        return MongoClient(database_url)

    def _get_collection(self) -> Collection:
        db = self._instance[self.db_name]
        return db[self.collection_name]

    def query_one(self, query: dict, ttl: int = 3600) -> dict:
        collection = self._get_collection()
        @cache_data(ttl=ttl)
        def _query(query: str) -> dict:
            return collection.find_one(query)
        return _query(query)

    def query_many(self, query: Union[None, dict], ttl: int = 3600) -> List[dict]:
        collection = self._get_collection()
        @cache_data(ttl=ttl)
        def _query(query: str) -> dict:
            if query is not None:
                return list(collection.find(query))
            else:
                return list(collection.find({}))     
        return _query(query)


    def insert_one(self, document: dict) -> None:
        collection = self._get_collection()
        collection.insert_one(document)

    def insert_many(self, documents: List[dict]) -> None:
        collection = self._get_collection()
        collection.insert_many(documents)

    def switch_database_collection(self, db_name: str, collection_name: str) -> None:
        self.db_name = db_name
        self.collection_name = collection_name
        
    def delete_one(self, query: dict) -> None:
        collection = self._get_collection()
        collection.delete_one(query)

    def delete_many(self, query: dict) -> None:
        collection = self._get_collection()
        collection.delete_many(query)

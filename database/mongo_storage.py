import os 
from dotenv import load_dotenv

from pymongo import MongoClient
import pandas as pd

load_dotenv()

db_name = "hotel_reviews"
collection_name = "reviews"
connection_string = os.getenv("MONGO_DB_CONNECTION_STRING")

class MongoDatabaseClient:
    def __init__(self,
                 connection_string=connection_string,
                 db_name= db_name,
                 collection_name=collection_name, 
                 ):
        self.connection_string = connection_string
        self.db_name = db_name
        self.collection_name=collection_name
        self.client = None 
        self.db = None
        self.collection = None 

    def connect(self):
        try:
            self.client = MongoClient(self.connection_string)
            self.db = self.client[self.db_name]
            self.collection = self.db[self.collection_name]

            self.client.admin.command("ping")
            return True 
        except Exception as e: 
            print("sum went wrong", e)
            return False 
        
    def insert_data(self, dataframe:pd.DataFrame):
        try: 
            if self.collection.estimated_document_count() > 0:
                response = input(f"Collection '{self.collection_name}' already contains data. Replace it? (y/N): ")
            if response.lower() == 'y':
                self.collection.drop()
                print("Existing collection dropped.")
            else:
                print("Appending to existing collection.")

            records = dataframe.to_dict('records')

            for record in records:
                for key, value in record.items():
                    if pd.isna(value):
                        record[key] = None

            batch_size = 1000
            total_inserted = 0
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                result = self.collection.insert_many(batch)
                total_inserted += len(result.inserted_ids)
                print(f"Inserted batch {i//batch_size + 1}: {len(result.inserted_ids)} records")
            
            print(f"Successfully inserted {total_inserted} total records")
            return True
        except Exception as e:
            print("Something went wrong inserting the data", e)

    def create_indexes(self):
        try:

            indexes = [
                [("Hotel_Name", 1)],
                [("Reviewer_Nationality", 1)],
                [("Reviewer_Score", 1)],
                [("Review_Date", -1)],
                [("Average_Score", 1)],
                [("Hotel_Name", 1), ("Reviewer_Score", 1)],  # Compound index
            ]
            
            for index in indexes:
                self.collection.create_index(index)
            
        except Exception as e:
            print("Something went wrong creating indexes", e)

    def init_mongodb_connection():
        client = MongoClient("mongodb://localhost:27017/")
        db = client["hotel_reviews"]
        return db["reviews"]
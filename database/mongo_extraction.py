import pandas as pd
import pymongo
from pymongo import MongoClient
import json
from datetime import datetime
import numpy as np

class HotelReviewsMongoSetup:
    def __init__(self, connection_string="mongodb://localhost:27017/", db_name="hotel_reviews", collection_name="reviews"):
        """
        Initialize MongoDB connection for hotel reviews data
        
        Args:
            connection_string (str): MongoDB connection string
            db_name (str): Database name
            collection_name (str): Collection name
        """
        self.connection_string = connection_string
        self.db_name = db_name
        self.collection_name = collection_name
        self.client = None
        self.db = None
        self.collection = None
    
    def connect(self):
        """Establish connection to MongoDB"""
        try:
            self.client = MongoClient(self.connection_string)
            self.db = self.client[self.db_name]
            self.collection = self.db[self.collection_name]
            
            # Test connection
            self.client.admin.command('ping')
            print(f"✅ Successfully connected to MongoDB")
            print(f"📊 Database: {self.db_name}")
            print(f"📁 Collection: {self.collection_name}")
            return True
        except Exception as e:
            print(f"❌ Failed to connect to MongoDB: {str(e)}")
            return False
    
    def load_csv_data(self, csv_file_path):
        """
        Load hotel reviews data from CSV file
        
        Args:
            csv_file_path (str): Path to the CSV file
        
        Returns:
            pd.DataFrame: Loaded and cleaned dataframe
        """
        try:
            print(f"📖 Loading data from {csv_file_path}")
            df = pd.read_csv(csv_file_path)
            
            # Clean and preprocess data
            df = self.preprocess_data(df)
            
            print(f"✅ Loaded {len(df)} records")
            return df
        except Exception as e:
            print(f"❌ Error loading CSV: {str(e)}")
            return None
    
    def preprocess_data(self, df):
        """
        Clean and preprocess the hotel reviews data
        
        Args:
            df (pd.DataFrame): Raw dataframe
        
        Returns:
            pd.DataFrame: Cleaned dataframe
        """
        print("🧹 Preprocessing data...")
        
        # Remove unnamed columns
        df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
        
        # Handle missing values
        df = df.fillna('')
        
        # Convert date columns
        if 'Review_Date' in df.columns:
            df['Review_Date'] = pd.to_datetime(df['Review_Date'], errors='coerce')
        
        # Handle numeric columns
        numeric_columns = ['Reviewer_Score', 'Average_Score', 'lat', 'lng', 
                          'Review_Total_Negative_Word_Counts', 'Review_Total_Positive_Word_Counts',
                          'Additional_Number_of_Scoring', 'Total_Number_of_Reviews',
                          'Total_Number_of_Reviews_Reviewer_Has_Given', 'days_since_review']
        
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Clean text columns
        text_columns = ['Negative_Review', 'Positive_Review', 'Tags']
        for col in text_columns:
            if col in df.columns:
                df[col] = df[col].astype(str).fillna('')
        
        # Create additional derived fields
        df['review_length_negative'] = df['Negative_Review'].str.len()
        df['review_length_positive'] = df['Positive_Review'].str.len()
        df['has_negative_review'] = df['Negative_Review'].str.len() > 0
        df['has_positive_review'] = df['Positive_Review'].str.len() > 0
        
        # Parse tags if they exist
        if 'Tags' in df.columns:
            df['parsed_tags'] = df['Tags'].apply(self.parse_tags)
        
        # Add ingestion timestamp
        df['ingested_at'] = datetime.utcnow()
        
        return df
    
    def parse_tags(self, tags_str):
        """
        Parse tags string into a list
        
        Args:
            tags_str (str): Tags string from CSV
        
        Returns:
            list: List of tags
        """
        if not tags_str or tags_str == '':
            return []
        
        try:
            # Remove brackets and quotes, split by comma
            cleaned = tags_str.strip("[]").replace("'", "").replace('"', '')
            tags = [tag.strip() for tag in cleaned.split(',') if tag.strip()]
            return tags
        except:
            return []
    
    def insert_data(self, df, batch_size=1000):
        """
        Insert dataframe data into MongoDB collection
        
        Args:
            df (pd.DataFrame): Data to insert
            batch_size (int): Batch size for insertion
        
        Returns:
            bool: Success status
        """
        try:
            print(f"📥 Inserting {len(df)} records into MongoDB...")
            
            # Convert DataFrame to list of dictionaries
            records = df.to_dict('records')
            
            # Handle NaN values for MongoDB
            for record in records:
                for key, value in record.items():
                    if pd.isna(value):
                        record[key] = None
            
            # Insert in batches
            total_inserted = 0
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                result = self.collection.insert_many(batch)
                total_inserted += len(result.inserted_ids)
                print(f"✅ Inserted batch {i//batch_size + 1}: {len(result.inserted_ids)} records")
            
            print(f"🎉 Successfully inserted {total_inserted} total records")
            return True
        except Exception as e:
            print(f"❌ Error inserting data: {str(e)}")
            return False
    
    def create_indexes(self):
        """Create useful indexes for better query performance"""
        try:
            print("🔍 Creating indexes...")
            
            # Create indexes for commonly queried fields
            indexes = [
                [("Hotel_Name", 1)],
                [("Reviewer_Nationality", 1)],
                [("Reviewer_Score", 1)],
                [("Review_Date", -1)],
                [("Average_Score", 1)],
                [("lat", 1), ("lng", 1)],  # Geospatial queries
                [("Hotel_Name", 1), ("Reviewer_Score", 1)],  # Compound index
            ]
            
            for index in indexes:
                self.collection.create_index(index)
                print(f"✅ Created index: {index}")
            
            print("🎉 All indexes created successfully")
        except Exception as e:
            print(f"❌ Error creating indexes: {str(e)}")
    
    def get_collection_stats(self):
        """Get statistics about the collection"""
        try:
            stats = self.db.command("collStats", self.collection_name)
            count = self.collection.count_documents({})
            
            print(f"\n📊 Collection Statistics:")
            print(f"Document count: {count:,}")
            print(f"Storage size: {stats.get('size', 0):,} bytes")
            print(f"Index count: {stats.get('nindexes', 0)}")
            
            # Sample aggregation queries
            print(f"\n🔍 Sample Aggregations:")
            
            # Top hotels by review count
            top_hotels = list(self.collection.aggregate([
                {"$group": {"_id": "$Hotel_Name", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
                {"$limit": 5}
            ]))
            print(f"Top 5 hotels by review count:")
            for hotel in top_hotels:
                print(f"  - {hotel['_id']}: {hotel['count']} reviews")
            
            # Average score by nationality
            avg_scores = list(self.collection.aggregate([
                {"$group": {"_id": "$Reviewer_Nationality", "avg_score": {"$avg": "$Reviewer_Score"}}},
                {"$sort": {"avg_score": -1}},
                {"$limit": 5}
            ]))
            print(f"Top 5 nationalities by average score:")
            for nation in avg_scores:
                print(f"  - {nation['_id']}: {nation['avg_score']:.2f}")
                
        except Exception as e:
            print(f"❌ Error getting stats: {str(e)}")
    
    def setup_sample_aggregation_pipelines(self):
        """Setup some sample aggregation pipelines for the dashboard"""
        print("\n🔧 Sample MongoDB Aggregation Pipelines for Dashboard:")
        
        pipelines = {
            "score_distribution": [
                {"$group": {"_id": {"$floor": "$Reviewer_Score"}, "count": {"$sum": 1}}},
                {"$sort": {"_id": 1}}
            ],
            
            "reviews_by_month": [
                {"$group": {
                    "_id": {
                        "year": {"$year": "$Review_Date"},
                        "month": {"$month": "$Review_Date"}
                    },
                    "count": {"$sum": 1}
                }},
                {"$sort": {"_id.year": 1, "_id.month": 1}}
            ],
            
            "hotel_performance": [
                {"$group": {
                    "_id": "$Hotel_Name",
                    "avg_score": {"$avg": "$Reviewer_Score"},
                    "review_count": {"$sum": 1},
                    "total_negative_words": {"$sum": "$Review_Total_Negative_Word_Counts"},
                    "total_positive_words": {"$sum": "$Review_Total_Positive_Word_Counts"}
                }},
                {"$match": {"review_count": {"$gte": 10}}},
                {"$sort": {"avg_score": -1}}
            ],
            
            "nationality_analysis": [
                {"$group": {
                    "_id": "$Reviewer_Nationality",
                    "count": {"$sum": 1},
                    "avg_score": {"$avg": "$Reviewer_Score"}
                }},
                {"$match": {"count": {"$gte": 5}}},
                {"$sort": {"count": -1}}
            ]
        }
        
        for name, pipeline in pipelines.items():
            print(f"\n{name.upper()}:")
            print(json.dumps(pipeline, indent=2))
    
    def close_connection(self):
        """Close MongoDB connection"""
        if self.client:
            self.client.close()
            print("📋 MongoDB connection closed")

def main():
    """Main function to setup MongoDB with hotel reviews data"""
    print("🏨 Hotel Reviews MongoDB Setup")
    print("=" * 50)
    
    # Initialize setup
    setup = HotelReviewsMongoSetup()
    
    # Connect to MongoDB
    if not setup.connect():
        return
    
    # Load data from CSV
    csv_file = "hotel_review_short.csv"  # Update this path
    df = setup.load_csv_data(csv_file)
    
    if df is None:
        return
    
    # Clear existing data (optional)
    response = input("Do you want to clear existing data? (y/N): ")
    if response.lower() == 'y':
        setup.collection.drop()
        print("🗑️ Existing data cleared")
    
    # Insert data
    if setup.insert_data(df):
        # Create indexes
        setup.create_indexes()
        
        # Show statistics
        setup.get_collection_stats()
        
        # Show sample aggregation pipelines
        setup.setup_sample_aggregation_pipelines()
    
    # Close connection
    setup.close_connection()
    
    print("\n🎉 Setup completed successfully!")
    print("You can now run the dashboard with: streamlit run dashboard.py")

if __name__ == "__main__":
    main()
import streamlit as st
from pymongo import MongoClient
import pandas as pd

@st.cache_resource
def init_mongodb_connection():
    client = MongoClient("mongodb://localhost:27017/")
    db = client["hotel_reviews"]
    return db["reviews"]

def load_data_from_mongodb(collection, filters=None):
    query = {}
    if filters:
        if 'hotel_name' in filters and filters['hotel_name']:
            query['Hotel_Name'] = {'$regex': filters['hotel_name'], '$options': 'i'}
        if 'nationality' in filters and filters['nationality']:
            query['Reviewer_Nationality'] = {'$regex': filters['nationality'], '$options': 'i'}
        if 'min_score' in filters and filters['min_score']:
            query['Reviewer_Score'] = {'$gte': filters['min_score']}
        if 'max_score' in filters and filters['max_score']:
            query['Reviewer_Score'] = {'$lte': filters['max_score']}

    data = list(collection.find(query))
    return pd.DataFrame(data) if data else pd.DataFrame()


collection = init_mongodb_connection()

filters = {
    "hotel_name": st.text_input("Hotel Name"),
    "nationality": st.text_input("Nationality"),
    "min_score": st.slider("Min Score", 0.0, 10.0, 5.0),
    "max_score": st.slider("Max Score", 0.0, 10.0, 10.0)
}

df = load_data_from_mongodb(collection, filters)
st.dataframe(df)

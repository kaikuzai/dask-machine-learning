import streamlit as st
from pymongo import MongoClient
import pandas as pd

st.set_page_config(page_title="YouTube Channel Dashboard", layout="wide")

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
    print(pd.DataFrame(data).head(2))
    return pd.DataFrame(data) if data else pd.DataFrame()


collection = init_mongodb_connection()


st.sidebar.header("Filters & Controls")
with st.sidebar:
    filters = {
        "hotel_name": st.text_input("Hotel Name, press enter to confirm"),
        "nationality": st.text_input("Nationality, press enter to confirm"),
        "min_score": st.slider("Min Score", 0.0, 10.0, 0.0),
        "max_score": st.slider("Max Score", 0.0, 10.0, 10.0)
    }

df = load_data_from_mongodb(collection, filters)

st.title("Hotel Reviews Dashboard")
st.markdown("""
This dashboard allows you to filter and explore hotel reviews stored in MongoDB.
Use the filters in the sidebar to narrow down your search by hotel name, reviewer nationality, and review score.
""")
try:
    st.success(f"Found {len(df)} reviews matching your criteria.")


    # Define metrics
    metrics = [
        ("Total Reviews", "Reviewer_Score", '#29b5e8'),  # Count
        ("Average Score", "Reviewer_Score", '#FF9F36'),  # Mean
        ("Most Common Nationality", "Reviewer_Nationality", '#D45B90'),
        ("Most Reviewed Hotel", "Hotel_Name", '#7D44CF')
    ]

    # Create 4 columns
    cols = st.columns(4)

    # Display metrics
    for col, (title, column, color) in zip(cols, metrics):
        if column == "Reviewer_Score" and "Average" in title:
            value = round(df[column].mean(), 2)
        elif column == "Reviewer_Score" and "Total" in title:
            value = df[column].count()
        elif column == "Reviewer_Nationality":
            value = df[column].mode().iat[0] if not df[column].empty else "N/A"
        elif column == "Hotel_Name":
            value = df[column].mode().iat[0] if not df[column].empty else "N/A"
        else:
            value = "N/A"

        col.metric(label=title, value=value)

    # Visualizations
    st.subheader("Reviewer Score Distribution")
    st.bar_chart(df["Reviewer_Score"].value_counts().sort_index())

    st.subheader("Top 10 Hotels by Number of Reviews")
    top_hotels = df["Hotel_Name"].value_counts().head(10)
    st.bar_chart(top_hotels)

    st.subheader("Top 10 Reviewer Nationalities")
    top_nationalities = df["Reviewer_Nationality"].value_counts().head(10)
    st.bar_chart(top_nationalities)
except Exception as e:
    st.error("something went wrong display graphs", e)

st.dataframe(df)
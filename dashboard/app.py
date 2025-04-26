# dashboard/app.py

import streamlit as st
from pymongo import MongoClient
from qdrant_client import QdrantClient
from sentence_transformers import SentenceTransformer

# SETUP
st.set_page_config(page_title="CrisisCast Dashboard", layout="wide")

# Mongo
mongo_client = MongoClient("mongodb://localhost:27017/")
db = mongo_client["crisiscast"]
collection = db["reddit_posts"]

# Qdrant
qdrant = QdrantClient(host="localhost", port=6333)
embedding_model = SentenceTransformer("all-MiniLM-L6-v2")
COLLECTION_NAME = "reddit_vectors"

# UI
st.title("CrisisCast Dashboard")
st.markdown("Monitor real-time global emergencies and explore them using semantic search.")

col1, col2 = st.columns(2)

# LIVE FEED
with col1:
    st.header("Live Feed (Latest Reddit Posts)")
    latest_posts = list(collection.find().sort("created_utc", -1).limit(10))
    for post in latest_posts:
        st.markdown(f"**{post.get('title', '')}**")
        st.write(f"Type: `{post.get('crisis_type', 'none')}` | Subreddit: `{post.get('subreddit', '')}`")
        st.markdown(f"[🔗 Source Link]({post.get('url', '#')})")
        st.divider()

# SEMANTIC SEARCH
with col2:
    st.header("🔍 Semantic Search")
    query = st.text_input("Enter search query (e.g., 'india flood', 'terrorist attack')")
    if query:
        vector = embedding_model.encode(query).tolist()
        results = qdrant.search(collection_name=COLLECTION_NAME, query_vector=vector, limit=5)

        for i, item in enumerate(results):
            payload = item.payload
            st.markdown(f"**Result {i+1} - Score: {round(item.score, 3)}**")
            st.write(f"Type: `{payload.get('crisis_type', 'none')}`")
            st.markdown(f"**{payload.get('title', '')}**")
            st.markdown(f"[🔗 Link]({payload.get('url', '#')})")
            st.divider()

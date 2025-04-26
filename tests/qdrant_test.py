from qdrant_client import QdrantClient
from qdrant_client.http.models import Distance, VectorParams, PointStruct

# 1. Connect to Qdrant
client = QdrantClient(host="localhost", port=6333)

# 2. Create a collection (only once)
client.recreate_collection(
    collection_name="crisis_vectors",
    vectors_config=VectorParams(size=5, distance=Distance.COSINE),  # Use actual vector size later
)

# 3. Insert a test vector
client.upsert(
    collection_name="crisis_vectors",
    points=[
        PointStruct(id=1, vector=[0.1, 0.2, 0.3, 0.4, 0.5], payload={"title": "Test post", "crisis_type": "war"})
    ]
)

# 4. Search similar vectors
hits = client.search(
    collection_name="crisis_vectors",
    query_vector=[0.1, 0.2, 0.3, 0.4, 0.5],
    limit=1
)

print("Search result:", hits)

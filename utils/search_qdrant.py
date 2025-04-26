from qdrant_client import QdrantClient
from sentence_transformers import SentenceTransformer

# Initialize
client = QdrantClient(host="localhost", port=6333)
model = SentenceTransformer("all-MiniLM-L6-v2")

def search_reddit_vectors(query_text, top_k=5):
    vector = model.encode(query_text).tolist()
    hits = client.search(
        collection_name="reddit_vectors",
        query_vector=vector,
        limit=top_k
    )

    print(f"\nTop {top_k} semantic results for: '{query_text}'")
    for i, hit in enumerate(hits, 1):
        print(f"\n🔹 Result {i}")
        print(f"🔍 Score: {hit.score:.4f}")
        print(f"🧠 Title: {hit.payload.get('title')}")
        print(f"🌐 Type: {hit.payload.get('crisis_type')}")
        print(f"🔗 URL: {hit.payload.get('url')}")

# Example run
if __name__ == "__main__":
    query = input("Enter your search query: ")
    search_reddit_vectors(query)

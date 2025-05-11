from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, current_timestamp
from pyspark.sql.types import StructType, StringType
import os
import requests
from dotenv import load_dotenv
from pymongo import MongoClient
from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient
from qdrant_client.models import PointStruct
import uuid
import time
import threading
import gc
import psutil
from requests.adapters import HTTPAdapter, Retry

# 1. Load environment variables
load_dotenv("config/.env")
JINA_TOKEN = os.getenv("JINA_API_KEY")
JINA_API_URL = "https://api.jina.ai/v1/classify"
JINA_HEADERS = {
    "Authorization": f"Bearer {JINA_TOKEN}",
    "Content-Type": "application/json",
    "Accept": "application/json"
}
ALLOWED = [
    "natural disaster",
    "terrorist attack",
    "cyber attack",
    "pandemic",
    "war",
    "financial crisis",
    "civil unrest",
    "infrastructure failure",
    "environmental crisis",
    "crime",
    "politics",
    "law",
    "other"
]

# Retry strategy for Jina API
session = requests.Session()
retry_strategy = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[502, 503, 504],
    allowed_methods=["POST"]
)
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("http://", adapter)
session.mount("https://", adapter)

# 2. Create Spark Session
spark = SparkSession.builder \
    .appName("KafkaCrisisClassifier") \
    .config("spark.master", "local[*]") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.driver.maxResultSize", "1g") \
    .config("spark.memory.offHeap.enabled", "true") \
    .config("spark.memory.offHeap.size", "2g") \
    .config("spark.cleaner.referenceTracking.cleanCheckpoints", "true") \
    .config("spark.streaming.backpressure.enabled", "true") \
    .config("spark.streaming.kafka.maxRatePerPartition", "100") \
    .config("spark.streaming.kafka.consumer.cache.enabled", "false") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 3. Define schema and read from Kafka
schema = StructType() \
    .add("id", StringType()) \
    .add("title", StringType()) \
    .add("timestamp", StringType(), True) \
    .add("author", StringType()) \
    .add("url", StringType()) \
    .add("source", StringType(), True)

df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9095,localhost:9096,localhost:9097") \
    .option("subscribe", "reddit_posts,bluesky_posts,google_news_posts") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .option("maxOffsetsPerTrigger", 1000) \
    .load()

df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_str") \
    .withColumn("data", from_json("json_str", schema)) \
    .select("data.*")

df_with_time = df_parsed.withColumn("processing_time", current_timestamp())

# 4. Setup MongoDB and Qdrant
mongo_client = MongoClient(
    "mongodb://localhost:27017/",
    maxPoolSize=50,
    connectTimeoutMS=5000,
    serverSelectionTimeoutMS=5000,
    retryWrites=True
)
db = mongo_client["crisiscast"]

qdrant = QdrantClient(host="localhost", port=6333)
COLLECTION_NAME = "post_vectors"
try:
    names = [c.name for c in qdrant.get_collections().collections]
    if COLLECTION_NAME not in names:
        qdrant.recreate_collection(
            collection_name=COLLECTION_NAME,
            vectors_config={"size": 384, "distance": "Cosine"}
        )
except Exception:
    qdrant.recreate_collection(
        collection_name=COLLECTION_NAME,
        vectors_config={"size": 384, "distance": "Cosine"}
    )

embedding_model = None

# 5. Batch sizes
MAX_CHUNK_SIZE = 100
VECTOR_BATCH_SIZE = 50

# 6. foreachBatch function

def write_to_all_outputs(df, epoch_id):
    global embedding_model
    start_time = time.time()

    # Collect micro-batch into pandas
    try:
        pandas_df = df.toPandas()
    except Exception as e:
        print(f"Error converting to Pandas: {e}")
        return
    records = pandas_df.to_dict("records")
    batch_size = len(records)
    if batch_size == 0:
        return
    print(f"Processing batch {epoch_id} with {batch_size} records")

    # Batch classification
    try:
        inputs = [{"text": r.get("title", "")} for r in records]
        payload = {"model": "jina-embeddings-v3", "labels": ALLOWED, "input": inputs}
        resp = session.post(JINA_API_URL, headers=JINA_HEADERS, json=payload, timeout=60)
        resp.raise_for_status()
        results = resp.json().get("data", [])
        for rec, item in zip(records, results):
            rec["crisis_type"] = item.get("prediction", "other").lower().strip()
    except Exception as e:
        print(f"Batch classification error: {e}")
        for rec in records:
            rec["crisis_type"] = "other"

    # Bulk insert into MongoDB
    try:
        db["unified_posts"].insert_many(records)
    except Exception as e:
        print(f"MongoDB bulk insert error: {e}")

    # Lazy init embedding model
    if embedding_model is None:
        try:
            embedding_model = SentenceTransformer("all-MiniLM-L6-v2")
        except Exception as e:
            print(f"Error initializing embedding model: {e}")
            return

    # Generate embeddings in batch
    texts = [r.get("title", "").strip() for r in records if r.get("title", "").strip()]
    try:
        vectors = embedding_model.encode(
            texts,
            batch_size=MAX_CHUNK_SIZE,
            show_progress_bar=False
        ).tolist()
    except Exception as e:
        print(f"Error encoding embeddings: {e}")
        vectors = []

    # Prepare Qdrant points
    points = []
    vec_idx = 0
    for rec in records:
        text = rec.get("title", "").strip()
        if not text or vec_idx >= len(vectors):
            continue
        try:
            points.append(
                PointStruct(
                    id=str(uuid.uuid4()),
                    vector=vectors[vec_idx],
                    payload={
                        "title": rec.get("title", ""),
                        "url": rec.get("url", ""),
                        "crisis_type": rec.get("crisis_type", "other"),
                        "source": rec.get("source", ""),
                        "id": rec.get("id", "")
                    }
                )
            )
        except Exception as e:
            print(f"Error creating PointStruct: {e}")
        vec_idx += 1

    # Bulk upsert to Qdrant
    for i in range(0, len(points), VECTOR_BATCH_SIZE):
        batch = points[i : i + VECTOR_BATCH_SIZE]
        try:
            if batch:
                qdrant.upsert(collection_name=COLLECTION_NAME, points=batch)
        except Exception as e:
            print(f"Qdrant upsert error: {e}")

    # Cleanup and log
    gc.collect()
    elapsed = time.time() - start_time
    print(
        f"Batch {epoch_id} processed {batch_size} records in {elapsed:.2f}s "
        f"({batch_size/elapsed:.2f} records/sec)"
    )

# 7. Memory monitoring thread
def monitor_memory():
    while True:
        gc.collect()
        proc = psutil.Process(os.getpid())
        print(f"Memory usage: {proc.memory_info().rss / 1024 / 1024:.2f} MB")
        time.sleep(60)

monitor_thread = threading.Thread(target=monitor_memory, daemon=True)
monitor_thread.start()

# 8. Start streaming on df_with_time
query = df_with_time.writeStream \
    .foreachBatch(write_to_all_outputs) \
    .option("checkpointLocation", "/tmp/checkpoints/reddit_stream") \
    .trigger(processingTime="5 seconds") \
    .start()

query.awaitTermination()

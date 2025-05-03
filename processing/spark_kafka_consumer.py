from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf, window, current_timestamp
from pyspark.sql.types import StructType, StringType, DoubleType
import os
import requests
import json
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
from functools import lru_cache

# 1. Load environment variables
load_dotenv("config/.env")
HF_TOKEN = os.getenv("HF_API_KEY")
HF_API_URL = "https://api-inference.huggingface.co/models/google/flan-t5-base"
HEADERS = {"Authorization": f"Bearer {HF_TOKEN}"}

# 2. Create Spark Session with improved configurations
spark = SparkSession.builder \
    .appName("RedditKafkaCrisisClassifier") \
    .config("spark.master", "spark://spark-master:7077") \
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

# Set log level to minimize unnecessary output
spark.sparkContext.setLogLevel("WARN")

# 3. Define schema for incoming JSON
schema = StructType() \
    .add("id", StringType()) \
    .add("title", StringType()) \
    .add("selftext", StringType()) \
    .add("created_utc", DoubleType()) \
    .add("author", StringType()) \
    .add("url", StringType()) \
    .add("subreddit", StringType())

# 4. Read from Kafka with updated configurations
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9095,localhost:9096,localhost:9097") \
    .option("subscribe", "reddit_posts") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .option("maxOffsetsPerTrigger", 1000) \
    .load()

# 5. Parse Kafka 'value' field from binary to JSON
df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_str") \
    .withColumn("data", from_json("json_str", schema)) \
    .select("data.*")

# Add timestamp for window operations and monitoring
df_with_time = df_parsed.withColumn("processing_time", current_timestamp())

# 6. Crisis Classification - IMPROVED to avoid API overload
# Create a classification cache to reduce API calls
classification_cache = {}
classification_lock = threading.Lock()

# Set up batched API calls - store pending requests
pending_requests = []
pending_lock = threading.Lock()
MAX_BATCH_SIZE = 10  # Maximum number of requests to batch
BATCH_TIMEOUT = 5  # Seconds to wait before processing a partial batch

# Create a thread-safe cache using lru_cache
@lru_cache(maxsize=1000)
def get_cached_classification(title):
    """Thread-safe cached classification lookup"""
    return classification_cache.get(title, None)

def add_to_classification_cache(title, classification):
    """Thread-safe cache update"""
    with classification_lock:
        classification_cache[title] = classification

# Function to process batched API requests
def process_batch_requests():
    global pending_requests
    
    while True:
        # Wait until we have some requests
        time.sleep(0.1)
        
        batch_to_process = None
        with pending_lock:
            # Check if we have enough requests or if timeout has elapsed
            if pending_requests and (len(pending_requests) >= MAX_BATCH_SIZE or 
                                     (time.time() - pending_requests[0]["timestamp"] > BATCH_TIMEOUT)):
                batch_to_process = pending_requests[:MAX_BATCH_SIZE]
                pending_requests = pending_requests[MAX_BATCH_SIZE:] if len(pending_requests) > MAX_BATCH_SIZE else []
        
        if batch_to_process:
            try:
                # Extract just the inputs
                prompts = [item["prompt"] for item in batch_to_process]
                
                # Make a single API call for multiple inputs
                response = requests.post(
                    HF_API_URL,
                    headers=HEADERS,
                    json={"inputs": prompts},
                    timeout=30
                )
                
                if response.status_code == 200:
                    results = response.json()
                    
                    # Process each result and update corresponding item
                    for i, result in enumerate(results):
                        if i < len(batch_to_process):
                            generated_text = result.get("generated_text", "").strip().lower()
                            item = batch_to_process[i]
                            
                            # Extract crisis type
                            crisis_type = "none"
                            allowed = ["natural_disaster", "terrorist_attack", "cyberattack", 
                                      "pandemic", "war", "financial_crisis", "none"]
                            
                            for label in allowed:
                                if label in generated_text:
                                    crisis_type = label
                                    break
                            
                            # Update result and cache
                            item["result"].append(crisis_type)
                            add_to_classification_cache(item["title"], crisis_type)
                else:
                    print(f"Batch API Error: {response.status_code} {response.text}")
                    # Fall back to "none" for all items in the batch
                    for item in batch_to_process:
                        item["result"].append("none")
                        add_to_classification_cache(item["title"], "none")
                        
            except Exception as e:
                print(f"Batch processing error: {e}")
                # Fall back to "none" for all items in the batch
                for item in batch_to_process:
                    item["result"].append("none")
                    add_to_classification_cache(item["title"], "none")

# Start the batch processing thread
batch_thread = threading.Thread(target=process_batch_requests, daemon=True)
batch_thread.start()

def classify_crisis_type_optimized(title, selftext):
    """Optimized classification function that uses batching and caching"""
    if not title:
        return "none"
    
    # Check cache first
    cached_result = get_cached_classification(title)
    if cached_result is not None:
        return cached_result
    
    # Create a shared result list for thread communication
    result_container = []
    
    # Create prompt
    prompt = f"Classify the type of crisis in the following sentence:\n{title}\nCrisis type:"
    
    # Add to pending requests
    request_item = {
        "title": title,
        "prompt": prompt,
        "result": result_container,
        "timestamp": time.time()
    }
    
    with pending_lock:
        pending_requests.append(request_item)
    
    # Wait for result with timeout
    start_time = time.time()
    while not result_container and time.time() - start_time < 10:
        time.sleep(0.1)
    
    # Return result or fallback
    if result_container:
        return result_container[0]
    else:
        # Fallback - store "none" in cache
        add_to_classification_cache(title, "none")
        return "none"

# 7. Register UDF
classify_crisis_udf = udf(classify_crisis_type_optimized, StringType())

# 8. Apply UDF to DataFrame
df_with_crisis_type = df_with_time.withColumn("crisis_type", classify_crisis_udf(col("title"), col("selftext")))

# 9. Setup MongoDB client with connection pooling and error handling
mongo_client = MongoClient(
    "mongodb://localhost:27017/",
    maxPoolSize=50,
    connectTimeoutMS=5000,
    serverSelectionTimeoutMS=5000,
    retryWrites=True
)
db = mongo_client["crisiscast"]
mongo_collection = db["reddit_posts"]

# 10. Setup Qdrant client and embedding model lazily to avoid driver memory issues
embedding_model = None
qdrant = QdrantClient(host="localhost", port=6333)
COLLECTION_NAME = "reddit_vectors"

# Check if collection exists before recreating
try:
    collections = qdrant.get_collections()
    collection_names = [c.name for c in collections.collections]
    if COLLECTION_NAME not in collection_names:
        qdrant.recreate_collection(
            collection_name=COLLECTION_NAME,
            vectors_config={"size": 384, "distance": "Cosine"}
        )
except Exception as e:
    print(f"Error checking/creating Qdrant collection: {e}")
    # Create collection anyway as fallback
    qdrant.recreate_collection(
        collection_name=COLLECTION_NAME,
        vectors_config={"size": 384, "distance": "Cosine"}
    )

# 11. Final batch write function with optimizations
def write_to_all_outputs(df, epoch_id):
    global embedding_model
    
    # Start timing
    start_time = time.time()
    batch_size = df.count()
    print(f"Processing batch {epoch_id} with {batch_size} records")
    
    # Initialize embedding model lazily
    if embedding_model is None:
        try:
            embedding_model = SentenceTransformer("all-MiniLM-L6-v2")
        except Exception as e:
            print(f"Error initializing embedding model: {e}")
            return
    
    # Process in smaller chunks to avoid memory issues
    MAX_CHUNK_SIZE = 100
    
    # Convert to pandas more efficiently by processing in chunks
    try:
        pandas_df = df.toPandas()
        data = pandas_df.to_dict("records")
    except Exception as e:
        print(f"Error converting to pandas: {e}")
        return
    
    if not data:
        return
    
    # Process in chunks
    for i in range(0, len(data), MAX_CHUNK_SIZE):
        chunk = data[i:i+MAX_CHUNK_SIZE]
        
        # MongoDB Insert with retry
        try:
            mongo_collection.insert_many(chunk, ordered=False)
        except Exception as e:
            print(f"MongoDB error: {e}")
            # Try one by one as fallback
            for doc in chunk:
                try:
                    mongo_collection.insert_one(doc)
                except Exception as e2:
                    print(f"Failed to insert document: {e2}")
        
        # Qdrant Insert
        points = []
        for row in chunk:
            try:
                text = f"{row.get('title', '')} {row.get('selftext', '')}".strip()
                if not text:
                    continue
                
                vector = embedding_model.encode(text).tolist()
                generated_id = str(uuid.uuid4())
                
                points.append(PointStruct(
                    id=generated_id,
                    vector=vector,
                    payload={
                        "title": row.get("title", ""),
                        "url": row.get("url", ""),
                        "crisis_type": row.get("crisis_type", "none"),
                        "reddit_id": row.get("id", "")
                    }
                ))
            except Exception as e:
                print(f"Error creating vector embedding: {e}")
        
        # Insert points in smaller batches
        VECTOR_BATCH_SIZE = 50
        for j in range(0, len(points), VECTOR_BATCH_SIZE):
            vector_batch = points[j:j+VECTOR_BATCH_SIZE]
            try:
                if vector_batch:
                    qdrant.upsert(
                        collection_name=COLLECTION_NAME,
                        points=vector_batch
                    )
            except Exception as e:
                print(f"Qdrant insert error: {e}")
    
    # Force garbage collection after batch processing
    gc.collect()
    
    # Log processing time
    end_time = time.time()
    elapsed = end_time - start_time
    print(f"Batch {epoch_id} processed {batch_size} records in {elapsed:.2f} seconds ({batch_size/elapsed:.2f} records/sec)")

# Monitor memory usage
def monitor_memory():
    while True:
        # Force garbage collection
        gc.collect()
        
        # Get process memory info
        process = psutil.Process(os.getpid())
        memory_info = process.memory_info()
        
        # Log memory usage
        print(f"Memory usage: {memory_info.rss / 1024 / 1024:.2f} MB")
        
        # Sleep for a minute
        time.sleep(60)

# Start memory monitoring in a separate thread
monitor_thread = threading.Thread(target=monitor_memory, daemon=True)
monitor_thread.start()

# 12. Start the Streaming Query with checkpoint and trigger
query = df_with_crisis_type.writeStream \
    .foreachBatch(write_to_all_outputs) \
    .option("checkpointLocation", "/tmp/checkpoints/reddit_stream") \
    .trigger(processingTime="5 seconds") \
    .start()

query.awaitTermination()
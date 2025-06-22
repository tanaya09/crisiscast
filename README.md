# CrisisCast 
> 🆘 Real-time crisis detection from social signals using scalable big data pipelines and AI.

> **Note:** For a more detailed analysis and screenshots, see the [Project Report](https://drive.google.com/file/d/1zY9Wx9fpCw1JWgF0teYs5nffHP10hglJ/view).

**CrisisCast** is a fault-tolerant, real-time social signal decoder for emergencies. It ingests, processes, classifies, and visualizes crisis-related content from multiple digital platforms, transforming raw social media and news feeds into actionable intelligence for emergency responders and decision-makers.

---

## 📑 Table of Contents

1. 🚀 [Features](#features)  
2. 🏗️ [Architecture](#architecture)  
3. 🛠️ [Tech Stack](#tech-stack)  
4. 📋 [Prerequisites](#prerequisites)  
5. ⚙️ [Usage](#usage)  
6. 📁 [Project Structure](#project-structure)  
7. ✨ [Future Enhancements](#future-enhancements)  
8. 👥 [Contributors](#contributors)  

---

## Features

- **Multi-source Ingestion**: Streams posts from Reddit, Bluesky (AT Protocol), and Google News RSS.  
- **Scalable Streaming**: Built on Apache Kafka (three-broker cluster) and Spark Structured Streaming for high-throughput, fault-tolerant data pipelines.  
- **AI-powered Classification**: FLAN-T5-base model hosted via FastAPI to classify posts into 13 crisis categories.  
- **Semantic Search**: Embeds text with SentenceTransformer (all-MiniLM-L6-v2) and stores vectors in Qdrant for low-latency, metadata-filtered similarity search.  
- **Interactive Dashboard**: Real-time visualizations (live feed, time-series, heatmaps, semantic search) built with Plotly Dash.  
- **Resilience & Monitoring**: Exponential backoff, Kafka replication, Spark checkpointing, memory monitoring, and Docker-based containerization.

---

## Architecture

1. **Ingestion Layer** (`ingestion/`):  
   - Custom Kafka producers serialize and publish JSON posts from each source.  
2. **Processing Layer** (`processing/` & `classification/`):  
   - Spark Structured Streaming consumes Kafka topics, enforces schema, timestamps records, and calls the classification API.  
   - Classification service (FastAPI + FLAN-T5) returns crisis labels and metadata.  
   - Batch embedding and upserts to Qdrant via the `utils/` helper module.  
3. **Storage**:  
   - **MongoDB**: Time-series store for enriched post metadata and classification labels.  
   - **Qdrant**: Vector database for similarity search with metadata filtering.  
4. **Visualization Layer** (`dashboard/`):  
   - Plotly Dash app retrieves data from MongoDB and Qdrant, rendering live feeds, trend charts, and semantic search results.  

---

## Tech Stack

- **Apache Kafka** – Distributed event streaming  
- **PySpark (Structured Streaming)** – Distributed micro-batch processing  
- **FastAPI** + **HuggingFace Transformers** (FLAN-T5) – Classification API  
- **SentenceTransformers** – Text embedding  
- **Qdrant** – Vector similarity search  
- **MongoDB** – NoSQL time-series storage  
- **Plotly Dash** – Interactive web dashboard  
- **Docker & Docker Compose** – Container orchestration  

---

## Prerequisites

- Docker & Docker Compose (≥ 1.29)  
- Python 3.8+  
- Java (for standalone Spark, if running locally)  
- MongoDB  
- Reddit API credentials  
- Bluesky account credentials
- pip install requirements.txt  

---

## Usage

- **Live Feed**: Displays the latest 20 posts (auto-refresh every 10 s).  
- **Trend Analysis**: Time-series charts of crisis mentions over selectable windows (6, 12, 24 h).  
- **Semantic Search**: Enter a free-text query to surface similar crisis posts via vector search.  
- **Filtering**: By source, crisis category, and time window.  

---

## Project Structure

```plaintext
crisiscast/
├── classification/        # FastAPI service for FLAN-T5 classification
├── consumer/              # Kafka consumers & MongoDB/Qdrant upserts
├── dashboard/             # Plotly Dash web application
├── ingestion/             # Kafka producers for Reddit, Bluesky, Google News
├── processing/            # Spark Structured Streaming job
├── utils/                 # Shared utility functions (schema, embedding, logging)
├── tests/                 # Unit and integration tests
├── docker-compose.yml     # Multi-container orchestration
├── requirements.txt       # Python dependencies
├── start_up.sh            # Helper script to launch components in order
└── README.md              # This document
```
## Future Enhancements

- GPU-accelerated model serving (e.g., Triton Inference Server)  
- Automated model fine-tuning with incoming data (RAG pipeline)  
- Kubernetes-based Spark and Kafka clusters for dynamic scaling  
- Multi-language support and expanded source coverage  
- Alerting and notification integration (Slack, SMS)

## Contributors

- Austin Huang 
- Nikhil Soni 
- Omer Basar 
- Sarasa Pattabiraman  
- Tanaya Pawar

import os
import time
import json
import requests
import feedparser
from kafka import KafkaProducer
from dateutil import parser
from datetime import timezone
from dotenv import load_dotenv

load_dotenv("config/.env")
RSS_URL       = os.getenv("NEWS_RSS_URL")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", 60))

def stream_news():
    seen = set()

    # set up Kafka producer exactly like the Bluesky script
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    print("🚀 Streaming RSS from", RSS_URL)
    while True:
        # fetch & parse
        feed = feedparser.parse(RSS_URL)
        if feed.bozo:
            resp = requests.get(RSS_URL)
            feed = feedparser.parse(resp.text)

        for e in feed.entries:
            uid = e.get("id") or e.link
            if uid in seen:
                continue
            seen.add(uid)

            # parse published timestamp into UTC ISO
            published_str = e.get("published") or e.get("updated") or ""
            try:
                dt = parser.parse(published_str)
                timestamp = dt.astimezone(timezone.utc).isoformat()
            except Exception:
                timestamp = None

            # pull publisher from <source> if author is missing
            publisher = None
            if hasattr(e, "source") and e.source:
                publisher = e.source.title

            data = {
                "id":        uid,
                "title":     e.title,
                "timestamp": timestamp,
                "author":    e.get("author") or publisher,
                "url":       e.link,
                "source":    "Google News RSS",
            }

            # print for debugging
            print(f"\n📌 {data['title']}")
            print(data)

            # send to Kafka on the 'google_news_posts' topic
            producer.send("google_news_posts", data)

        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    try:
        stream_news()
    except KeyboardInterrupt:
        print("\n🛑 Stream stopped by user")

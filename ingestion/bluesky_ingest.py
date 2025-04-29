import os
import time
import json
from atproto import Client
from atproto_client.exceptions import RequestException, BadRequestError
from kafka import KafkaProducer
from dotenv import load_dotenv
from dateutil import parser
from datetime import datetime, timezone

load_dotenv("config/.env")
HANDLE        = os.getenv("BLUESKY_USERNAME")
PASSWORD      = os.getenv("BLUESKY_PASSWORD")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", 30))

def at_uri_to_url(at_uri: str) -> str:
    path = at_uri.replace("at://", "")
    authority, _, rkey = path.split("/", 2)
    return f"https://bsky.app/profile/{authority}/post/{rkey}"

def stream_whats_hot():
    client = Client()
    client.login(HANDLE, PASSWORD)

    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    feed_uri       = (
        "at://did:plc:z72i7hdynmk6r22z27h6tvur/"
        "app.bsky.feed.generator/whats-hot"
    )
    cursor         = None
    seen_uris      = set()
    allowed_topics = {"t-news", "nettop"}

    print("🚀 Streaming Bluesky “What’s Hot”…")
    while True:
        params = {"feed": feed_uri, "limit": 50}
        if cursor:
            params["cursor"] = cursor

        # resilience to 503 and 400 errors
        backoff = 1
        resp = None
        while True:
            try:
                resp = client.app.bsky.feed.get_feed(params)
                break
            except BadRequestError as e:
                # 400 InvalidRequest: likely a bad cursor—reset & skip
                print("⚠️ 400 InvalidRequest from Bluesky, resetting cursor and skipping this round.")
                cursor = None
                break
            except RequestException as e:
                # catch other API errors
                code = e.response.status_code if e.response else None
                if getattr(e, "error", None) == "NotEnoughResources" or code == 503:
                    print(f"⚠️ 503 from Bluesky, backing off {backoff}s…")
                    time.sleep(backoff)
                    backoff = min(backoff * 2, 60)
                    continue
                # re-raise everything else
                raise

        # if we never got a resp (due to 400), skip processing
        if resp is None:
            time.sleep(POLL_INTERVAL)
            continue

        posts  = resp.feed
        cursor = resp.cursor

        for item in posts:
            if item.feed_context not in allowed_topics:
                continue
            if not item.post.record.text or not item.post.record.text.strip():
                continue

            uri = item.post.uri
            if uri in seen_uris:
                continue
            seen_uris.add(uri)

            post = item.post
            data = {
                "id":        post.cid,
                "title":     post.record.text,
                "timestamp": parser.isoparse(post.indexed_at)
                                        .astimezone(timezone.utc)
                                        .isoformat(),
                "author":    post.author.handle,
                "url":       at_uri_to_url(post.uri),
                "source":    "Bluesky: What's Hot",
            }

            print(f"\n📌 {data['title']}")
            print(data)
            producer.send("bluesky_posts", data)

        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    try:
        stream_whats_hot()
    except KeyboardInterrupt:
        print("\n🛑 Stream stopped by user")

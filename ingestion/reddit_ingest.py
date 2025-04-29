import os
import time
import praw
from dotenv import load_dotenv
import json
from kafka import KafkaProducer
# Load secrets
load_dotenv("config/.env")
print(":key: Username loaded:", os.getenv("REDDIT_USERNAME"))
print(":key: Client ID loaded:", os.getenv("REDDIT_CLIENT_ID")[:4], "***")
# Setup Reddit instance
reddit = praw.Reddit(
    client_id=os.getenv("REDDIT_CLIENT_ID"),
    client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
    user_agent=os.getenv("REDDIT_USER_AGENT"),
    username=os.getenv("REDDIT_USERNAME"),
    password=os.getenv("REDDIT_PASSWORD")
)
try:
    print(":white_check_mark: Logged in as:", reddit.user.me())
except Exception as e:
    print(":x: Login failed:", e)
producer = KafkaProducer(
    bootstrap_servers = ['localhost:9095', 'localhost:9096', 'localhost:9097'],
    value_serializer = lambda v: json.dumps(v).encode('utf-8')
)
def main():
    print(":rocket: Connected to Reddit API. Listening to subreddit...")
    # subreddit = reddit.subreddit("worldnews")
    subreddit = reddit.subreddit("news+worldnews+technology")
    # for submission in subreddit.stream.submissions(skip_existing=True):
    for submission in subreddit.stream.submissions():
        post = {
            "id": submission.id,
            "title": submission.title,
            "selftext": submission.selftext,
            "created_utc": submission.created_utc,
            "author": str(submission.author),
            "url": submission.url,
            "subreddit": str(submission.subreddit),
        }
        print(f"\n:pushpin: {post['title']}")
        print(post)
        # Send to Kafka
        producer.send("reddit_posts", post)
if __name__ == "__main__":
    main()
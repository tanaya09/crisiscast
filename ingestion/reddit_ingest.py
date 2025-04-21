import os
# import time
import praw
from dotenv import load_dotenv

# Load secrets
load_dotenv("config/.env")
print("🔑 Username loaded:", os.getenv("REDDIT_USERNAME"))
print("🔑 Client ID loaded:", os.getenv("REDDIT_CLIENT_ID")[:4], "***")

# Setup Reddit instance
reddit = praw.Reddit(
    client_id=os.getenv("REDDIT_CLIENT_ID"),
    client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
    user_agent=os.getenv("REDDIT_USER_AGENT"),
    username=os.getenv("REDDIT_USERNAME"),
    password=os.getenv("REDDIT_PASSWORD")
)

try:
    print("✅ Logged in as:", reddit.user.me())
except Exception as e:
    print("❌ Login failed:", e)


def main():
    print("🚀 Connected to Reddit API. Listening to subreddit...")
    # subreddit = reddit.subreddit("worldnews")
    subreddit = reddit.subreddit("news+worldnews+technology")

    for submission in subreddit.stream.submissions():
        print(f"\n📌 {submission.title}")
        # post = {
        #     "id": submission.id, # Unique Reddit post ID
        #     "title": submission.title, # Post title
        #     "selftext": submission.selftext, # Text body (if any)
        #     "created_utc": submission.created_utc, # Unix timestamp of creation
        #     "author": str(submission.author), # Author's username
        #     "url": submission.url, # URL to content (or post itself)
        #     "subreddit": str(submission.subreddit), # Subreddit name
        # }
        post = {
        "id": submission.id,
        # "title": submission.title,
        # "selftext": submission.selftext,
        "created_utc": submission.created_utc,
        # "author": str(submission.author),
        # "url": submission.url,
        "subreddit": str(submission.subreddit),
        "score": submission.score,
        "num_comments": submission.num_comments,
        # "flair": submission.link_flair_text,
        # "permalink": submission.permalink,
        "over_18": submission.over_18
        }
        print(post)

if __name__ == "__main__":
    main()

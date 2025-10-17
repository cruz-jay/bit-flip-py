from dotenv import load_dotenv
import os
import praw
from praw import Reddit

load_dotenv(".env")
client = os.getenv("REDDIT_CLIENT", "")
secret = os.getenv("REDDIT_KEY", "")

def fetch_reddit_submission(subreddit):
    reddit = praw.Reddit(
        client_id=client,
        client_secret=secret,
        user_agent="reddit",
    )

    subreddit = reddit.subreddit("wallstreetbets")

    for submission in subreddit.new(limit=3):
        print(f"Title: {submission.title}")
        print(f"Score: {submission.score}")
        print(f"Comments: {submission.num_comments}")
        print(f"Author: {submission.author}")
        print(f"Time: {submission.created_utc}")
        print(f"Text: {submission.selftext}")
        print("---")


if __name__ == "__main__":
    fetch_reddit_submission("wallstreetbets")


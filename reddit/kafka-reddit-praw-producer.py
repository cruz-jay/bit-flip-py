import os
import logging
import re
import time
import pandas as pd
import redis
import praw
import json
from datetime import datetime
from quixstreams import Application
from dotenv import load_dotenv
from praw.exceptions import APIException

load_dotenv(".env")
CLIENT = os.getenv("REDDIT_CLIENT", "")
SECRET = os.getenv("REDDIT_KEY", "")
USERNAME = os.getenv("REDDIT_USER", "")

# ===============================
# Redis Config
# ===============================

r = None

def connect_redis():
    global r
    try:
        r = redis.Redis(host='localhost', port=6379, db=0, socket_connect_timeout=5)
        r.ping()
        logging.info("Connected to Redis")
    except redis.RedisError as e:
        logging.error(f"Redis connection failed: {e} - Falling back to local deduping")
        r = None

# ===============================
# CSV FETCH
# ===============================
def clean_name(company_name):
    if not company_name:
        return ""

    name = company_name.strip()

    replacements = {
        ' Inc.': '', ' Inc': '', ' Corporation': '', ' Corp.': '', ' Corp': '',
        ' Company': '', ' Co.': '', ' Co': '', ' Ltd.': '', ' Ltd': '',
        ' Limited': '', ' plc': '', ' PLC': '', ' Group': '', ' (The)': '', 'The ': '',
    }

    for old, new in replacements.items():
        name = name.replace(old, new)

    return name.strip()


def load_sp500(csv_path="constituents.csv"):
    df = pd.read_csv(csv_path)
    symbols = df["Symbol"].str.upper().tolist()
    names = []

    for _, row in df.iterrows():
        cleaned = clean_name(row["Security"])
        if cleaned and len(cleaned.split()) > 1:
            names.append(f'"cleaned.upper()"')
        elif cleaned:
            names.append(cleaned.upper())

    search_terms = set(symbols + names)
    logging.info(f"Loaded {len(search_terms)} search terms from {csv_path}")
    return search_terms


def count_tickers(content, search_terms):
    if not content:
        return {}
    words = re.findall(r'\b[A-Z]{3,5}\b', content.upper())
    counts = {term: words.count(term) for term in search_terms if term in words}
    return {k: v for k, v in counts.items() if v > 0}  # Only non-zero


# ===============================
# PRAW VERSION
# REDDIT POSTS FETCH
# ===============================
SUBREDDIT_NAME = "wallstreetbets"
SEEN_TTL_SECONDS = 7 * 24 * 3600  # 7 days

def reddit_posts_praw(search_terms):
    reddit = praw.Reddit(client_id=CLIENT, client_secret=SECRET, user_agent="MyRedditApp.0.0.1")
    subreddit = reddit.subreddit(SUBREDDIT_NAME)
    posts = []
    seen_posts_local = set()
    redis_seen_key = f"reddit:seen_posts:{SUBREDDIT_NAME}"

    batch_size = 20
    term_list = list(search_terms)
    for i in range(0, len(term_list), batch_size):
        batch = term_list[i:i + batch_size]
        query = " OR ".join(batch)
        logging.info(f"Searching batch {i // batch_size + 1}: {query[:50]}...")

        try:
            for submission in subreddit.search(
                    query=query,
                    limit=100,
                    sort="top",
                    time_filter="month"
            ):
                post_id = submission.id

                if post_id in seen_posts_local:
                    logging.debug(f"Local dupe: {post_id}")
                    continue

                # Redis deduping
                is_new = True
                if r:
                    try:
                        added = r.sadd(redis_seen_key, post_id)
                        if added == 1:
                            logging.debug(f"Redis added new: {post_id}")
                            r.expire(redis_seen_key, SEEN_TTL_SECONDS)
                        else:  # Dupe (added==0)
                            logging.debug(f"Redis dupe: {post_id}")
                            is_new = False
                    except redis.RedisError as e:
                        logging.warning(f"Redis error for {post_id}: {e} - Using local")
                else:
                    logging.debug("No Redis - using local only")

                if not is_new:
                    continue

                seen_posts_local.add(post_id)  # Tracks locally

                content = submission.title + " " + submission.selftext
                mentions = count_tickers(content, search_terms)

                if mentions:
                    post_msg = {
                        "id": post_id,
                        "type": "post",
                        "ticker": list(mentions.keys())[0] if mentions else "",
                        "content": content,
                        "author": str(submission.author),
                        "score": submission.score,
                        "num_comments": submission.num_comments,
                        "url": submission.shortlink,
                        "created_utc": int(submission.created_utc),
                        "created": datetime.fromtimestamp(submission.created_utc).isoformat(),
                        "ticker_mentions": mentions
                    }
                    posts.append(post_msg)

        except APIException as e:
            logging.error(f"PRAW API error in batch {i}: {e}")
            time.sleep(60)
        except Exception as e:
            logging.error(f"Unexpected error in batch {i}: {e}")

        time.sleep(1)

    logging.info(f"Found {len(posts)} posts --deduped")
    return posts


# ===============================
# KAFKA PRODUCER
# ===============================
def kafka_producer(posts):
    app = Application(
        broker_address="localhost:9092",
        loglevel="INFO",
    )
    with app.get_producer() as producer:
        for post in posts:
            try:
                producer.produce(
                    topic="reddit-wsb-posts-kafka",
                    key=post["id"].encode("utf-8"),
                    value=json.dumps(post).encode("utf-8"),
                )
                logging.debug(f"Produced: {post['id']}")
            except Exception as e:
                logging.error(f"Error producing {post['id']}: {e}")
        producer.flush()
        logging.info(f"Produced {len(posts)} messages")


def main():
    connect_redis()
    # logging.info("Starting producer...")
    sp500_companies = load_sp500("constituents.csv")
    logging.info(f"Starting Reddit search for {len(sp500_companies)} terms...")

    messages = reddit_posts_praw(sp500_companies)
    logging.info(f"Collected {len(messages)} total messages")

    if messages:
        kafka_producer(messages)
    else:
        logging.info("No messages to produce")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logging.info("LOG MESSAGE - Start")
    try:
        main()
    except Exception as e:
        logging.exception("Fatal error:")
        raise
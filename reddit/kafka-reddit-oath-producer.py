import os
import logging
import json
import requests
import pandas as pd
from quixstreams import Application
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv(".env")
CLIENT = os.getenv("REDDIT_CLIENT", "")
SECRET = os.getenv("REDDIT_KEY", "")
USERNAME = os.getenv("REDDIT_USER", "")

# ===============================
# TIME CONFIGURATION
# ===============================
def get_today():
    return datetime.today().strftime("%Y-%m-%d")

def get_yesterday():
    return (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")

def get_thirty_days_ago():
    today = datetime.today()
    thirty_days_ago = today - timedelta(days=29)
    return thirty_days_ago.strftime("%Y-%m-%d")

def get_unix_datetime(date_str):
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    return int(dt.timestamp())

# ===============================
# CSV FETCH
# ===============================
def clean_name(company_name):
    if not company_name:
        return ""

    name = company_name.strip()

    replacements = {
        ' Inc.': '',
        ' Inc': '',
        ' Corporation': '',
        ' Corp.': '',
        ' Corp': '',
        ' Company': '',
        ' Co.': '',
        ' Co': '',
        ' Ltd.': '',
        ' Ltd': '',
        ' Limited': '',
        ' plc': '',
        ' PLC': '',
        ' Group': '',
        ' (The)': '',
        'The ': '',
    }

    for old, new in replacements.items():
        name = name.replace(old, new)

    return name.strip()

def load_sp500(csv_path="constituents.csv"):
    df = pd.read_csv(csv_path)
    data = []
    for _, row in df.iterrows():
        original_name = row["Security"]
        name = clean_name(original_name)
        data.append([name])

    flat_list = [item for sublist in data for item in sublist]

    return flat_list

# ===============================
# OATH VERSION!
# REDDIT POST & COMMENT FETCH
# ===============================
def access_token():
    auth = requests.auth.HTTPBasicAuth(CLIENT, SECRET)

    data = {
        "grant_type": "client_credentials"
    }

    headers = {
        "User-Agent": f"MyRedditApp.0.0.1 ({USERNAME})"
    }

    response = requests.post(
        "https://www.reddit.com/api/v1/access_token",
        auth=auth,
        data=data,
        headers=headers,
    )
    if response.status_code != 200:
        logging.error(f"Error fetching access token: {response.text}")

    token = response.json()["access_token"]
    return token

SUBREDDIT = "wallstreetbets"
START_TIMESTAMP = get_thirty_days_ago()
END_TIMESTAMP = get_yesterday()
def reddit_posts_and_comments_Oath(access_token, sp500_companies):
    all_posts = []
    for ticker in sp500_companies:
        url = f"https://oauth.reddit.com/r/{SUBREDDIT}/search"

        headers = {
            "Authorization": f"Bearer {access_token}",
            "User-Agent": f"MyRedditApp.0.0.1 ({USERNAME})",
        }

        params = {
            "q": ticker,
            "sort": "new",
            "limit": 10,
            "restrict_sr": "on",
            "after": get_unix_datetime(START_TIMESTAMP),
            "before": get_unix_datetime(END_TIMESTAMP),
            "syntax": "cloudsearch",
        }

        response = requests.get(url, headers=headers, params=params)
        if response.status_code != 200:
            logging.error(f"Error fetching {ticker}: {response.text}")
        else:
            data = response.json()
            if "data" in data and "children" in data["data"]:
                posts = data["data"]["children"]
                for post in posts:
                    post_data = post.get("data", {})
                    all_posts.append({
                        "id": post_data.get("id"),
                        "title": post_data.get("title"),
                        "selftext": post_data.get("selftext"),
                        "score": post_data.get("score"),
                        "created_utc": post_data.get("created_utc"),
                        "ticker": ticker,
                    })
                logging.info(f"Fetched {len(posts)} posts for {ticker}")
        return all_posts


# ===============================
# KAFKA PRODUCER
# ===============================
def kafka_producer(messages):
    app = Application(
        broker_address="localhost:9092",
        loglevel="INFO",
    )
    with app.get_producer() as producer:
        for message in messages:
            try:
                producer.produce(
                    topic="reddit-posts-comments-kafka",
                    key=message['id'],
                    value=json.dumps(message).encode("utf-8"),
                )
                logging.debug(f"Producer produced message: {message["id"]}")
            except Exception as e:
                logging.error(f"Error producing message {message.get('id')}: {e}")
        producer.flush()
        logging.info(f"Producer flushed {len(messages)} messages")

def main():
    logging.info("Starting S&P 500 news fetch...")
    sp500_companies = load_sp500("constituents.csv")

    token = access_token()
    messages = reddit_posts_and_comments_Oath(token, sp500_companies)

    if messages:
        kafka_producer(messages)

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    main()

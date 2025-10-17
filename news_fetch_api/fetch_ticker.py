import requests
import time
import json
import logging
import csv
import os
from quixstreams import Application
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()


# ---------- Utility Functions ----------

def get_today():
    return datetime.today().strftime("%Y-%m-%d")

def get_yesterday():
    return (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")

def get_first_day_of_current_month():
    today = datetime.today()
    return today.replace(day=1).strftime("%Y-%m-%d")

def get_same_day_last_year():
    today = datetime.today()
    last_year = today.replace(year=today.year - 1)
    return last_year.strftime("%Y-%m-%d")

def get_thirty_days_ago():
    today = datetime.today()
    thirty_days_ago = today - timedelta(days=29)
    return thirty_days_ago.strftime("%Y-%m-%d")

# ---------- Read/Write Functions ----------

def save_to_txt(data, query, filename):
    """Save JSON data to a text file"""
    os.makedirs(query, exist_ok=True)
    filepath = os.path.join(query, filename)


    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"Data saved to {filepath}")


def save_to_csv(data, query, filename):
    """Save CSV data to a text file"""
    os.makedirs(query, exist_ok=True)
    filepath = os.path.join(query, filename)

    articles = data.get('articles', [])
    if not articles:
        return

    fieldnames = ['source_id', 'source_name', 'author', 'title', 'description',
                  'url', 'urlToImage', 'publishedAt', 'content']

    file_exists = os.path.exists(filename)

    with open(filepath, 'a', newline='', encoding='utf-8') as f:  # 'a' for append
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()

        for article in articles:
            row = {
                'source_id': article.get('source', {}).get('id', ''),
                'source_name': article.get('source', {}).get('name', ''),
                'author': article.get('author', ''),
                'title': article.get('title', ''),
                'description': article.get('description', ''),
                'url': article.get('url', ''),
                'urlToImage': article.get('urlToImage', ''),
                'publishedAt': article.get('publishedAt', ''),
                'content': article.get('content', '')
            }
            writer.writerow(row)

    print(f"Data saved to {filepath}")


def get_fetch_state(filename="fetch_state.json"):
    """Read fetch state from JSON file"""
    if os.path.exists(filename):
        with open(filename, 'r') as f:
            state = json.load(f)
            return state.get('last_fetch_time'), state.get('article_count', 0)

    default_time = (datetime.today() - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%SZ")
    return default_time, 0


def save_fetch_state(timestamp, article_count, filename="fetch_state.json"):
    """Save fetch state to JSON file"""
    state = {
        'last_fetch_time': timestamp,
        'article_count': article_count,
        'updated_at': datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    }
    with open(filename, 'w') as f:
        json.dump(state, f, indent=2)


# ---------- Fetch Functions ----------

def get_news():

    from_date = get_thirty_days_ago()
    to_date = get_today()

    domains = ",".join([
        "reuters.com",
        "marketwatch.com",
        "wsj.com",
        "bloomberg.com",
        "fortune.com",
        # "forbes.com",
        "businessinsider.com",
        "fool.com",
        "investing.com",
        "seekingalpha.com",
    ])
    query = "apple"
    response = requests.get(
        "https://newsapi.org/v2/everything",
        params={
            "q": query,
            "domains": "forbes.com",
            "language": "en",
            "sortBy": "publishedAt",
            "searchIn": "title",
            "from": from_date,
            "to": to_date,
            "apiKey": os.getenv("NEWS_API_KEY", ""),
        },
    )

    data = response.json()

    timestamp = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")

    filename_txt = f"{query.upper()}_NEWS_{timestamp}.txt"
    filename_csv = f"{query.upper()}_NEWS_{timestamp}.csv"

    save_to_txt(data, query, filename_txt)
    save_to_csv(data, query, filename_csv)
    return data

def main():
    app = Application(
        broker_address="localhost:9092",
        loglevel="DEBUG",
    )
    with app.get_producer() as producer:
        while True:
            news = get_news()
            logging.debug("Got the news: %s", news)
            producer.produce(
                topic="news_data_demo",
                key="apple",
                value=json.dumps(news),
            )
            logging.info("Produced. Sleeping...")
            time.sleep(300)

if __name__ == "__main__":
    logging.basicConfig(level="DEBUG")
    main()
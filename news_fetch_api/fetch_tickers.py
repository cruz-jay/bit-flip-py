import requests
import time
import json
import logging
import csv
import os
import pandas as pd
from quixstreams import Application
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()


def load_sp500_companies(csv_path="constituents.csv"):
    """Load S&P 500 companies (symbol + name) from CSV file"""
    df = pd.read_csv(csv_path)
    companies = []

    for _, row in df.iterrows():
        symbol = row['Symbol']
        security = row['Security']

        # Clean up company name for better matching
        clean_name = security
        if clean_name.startswith('The '):
            clean_name = clean_name[4:]  # Remove "The " prefix
        clean_name = clean_name.replace(' Inc.', '').replace(' Inc', '')
        clean_name = clean_name.replace(' Corp.', '').replace(' Corp', '')
        clean_name = clean_name.replace(' Ltd.', '').replace(' Ltd', '')
        clean_name = clean_name.replace(' PLC', '').replace(' Co.', '')

        companies.append({
            'symbol': symbol,
            'name': security,  # Full name with legal suffixes
            'search_name': clean_name  # Simplified name for search
        })

    logging.info(f"Loaded {len(companies)} S&P 500 companies")
    return companies


def get_today():
    """Returns today's date in YYYY-MM-DD format"""
    return datetime.today().strftime("%Y-%m-%d")


def get_yesterday():
    """Returns yesterday's date in YYYY-MM-DD format"""
    return (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")


def get_thirty_days_ago():
    """Returns date 30 days ago in YYYY-MM-DD format"""
    today = datetime.today()
    thirty_days_ago = today - timedelta(days=29)
    return thirty_days_ago.strftime("%Y-%m-%d")


def save_batch_summary(results, filename="data/fetch_summary.json"):
    """
    Save summary statistics of fetch batch as JSON
    Includes total tickers, articles, and timestamps
    """
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2)
    logging.info(f"Batch summary saved to {filename}")


def fetch_news_for_ticker(ticker, from_date, to_date, domains):
    """
    Fetch news for a single ticker from NewsAPI
    Returns list of article dictionaries
    """
    try:
        response = requests.get(
            "https://newsapi.org/v2/everything",
            params={
                "q": ticker,
                "domains": domains,
                "language": "en",
                "sortBy": "publishedAt",
                "searchIn": "title,description",
                "from": from_date,
                "to": to_date,
                "pageSize": 100,
                "apiKey": os.getenv("NEWS_API_KEY", ""),
            },
            timeout=10
        )

        if response.status_code == 200:
            data = response.json()
            articles = data.get('articles', [])
            logging.info(f"{ticker}: Found {len(articles)} articles")
            return articles
        else:
            logging.error(f"{ticker}: API error {response.status_code}")
            return []

    except Exception as e:
        logging.error(f"{ticker}: Exception {str(e)}")
        return []


def create_raw_news_message(article, primary_ticker, all_mentioned_tickers):
    """
    Create standardized message for raw-news topic
    Converts NewsAPI format to custom schema
    """
    return {
        "primary_ticker": primary_ticker,
        "mentioned_tickers": all_mentioned_tickers,
        "title": article.get('title', ''),
        "description": article.get('description', ''),
        "content": article.get('content', ''),
        "url": article.get('url', ''),
        "source": article.get('source', {}).get('name', ''),
        "published_at": article.get('publishedAt', ''),
        "fetched_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def get_all_news(sp500_companies):
    """
    Fetch news for all S&P 500 tickers
    Returns list of standardized messages and grouped by ticker
    """
    from_date = get_thirty_days_ago()
    to_date = get_today()

    domains = ",".join([
        "reuters.com",
        "marketwatch.com",
        "wsj.com",
        "bloomberg.com",
        "fortune.com",
        "forbes.com",
        "businessinsider.com",
        "fool.com",
        "investing.com",
        "seekingalpha.com",
    ])

    all_messages = []
    messages_by_ticker = {}

    fetch_stats = {
        "total_tickers": len(sp500_companies),
        "tickers_processed": 0,
        "total_articles": 0,
        "start_time": datetime.utcnow().isoformat(),
    }

    for i, company in enumerate(sp500_companies, 1):
        ticker_symbol = company['symbol']
        logging.info(f"Processing {i}/{len(sp500_companies)}: {ticker_symbol}")

        articles = fetch_news_for_ticker(ticker_symbol, from_date, to_date, domains)

        ticker_messages = []

        for article in articles:
            full_text = f"{article.get('title', '')} {article.get('description', '')} {article.get('content', '')}"

            message = create_raw_news_message(
                article=article,
                primary_ticker=ticker_symbol,
                all_mentioned_tickers=[ticker_symbol]  # You'll need to implement ticker extraction
            )

            all_messages.append(message)
            ticker_messages.append(message)

        messages_by_ticker[ticker_symbol] = ticker_messages
        fetch_stats["tickers_processed"] += 1
        fetch_stats["total_articles"] += len(articles)

        if i < len(sp500_companies):
            time.sleep(1.5)

    fetch_stats["end_time"] = datetime.utcnow().isoformat()
    save_batch_summary(fetch_stats)

    logging.info(
        f"Fetch complete: {fetch_stats['total_articles']} articles from {fetch_stats['tickers_processed']} tickers")

    return all_messages, messages_by_ticker


def produce_to_kafka(messages):
    """
    Send all messages to Kafka raw-news topic
    Partitions by primary_ticker for parallel processing
    """
    app = Application(
        broker_address="localhost:9092",
        loglevel="INFO",
    )

    with app.get_producer() as producer:
        for msg in messages:
            try:
                producer.produce(
                    topic="raw-news",
                    key=msg["primary_ticker"],
                    value=json.dumps(msg),
                )
                logging.debug(f"Produced: {msg['primary_ticker']} - {msg['title'][:50]}...")
            except Exception as e:
                logging.error(f"Failed to produce message: {str(e)}")

        producer.flush()
        logging.info(f"Successfully produced {len(messages)} messages to Kafka")


def main():
    """
    Main execution flow:
    1. Load S&P 500 tickers
    2. Fetch news for all tickers
    3. Save to CSV and TXT files
    4. Send to Kafka
    """
    logging.info("Starting S&P 500 news fetch...")

    sp500_companies = load_sp500_companies("constituents.csv")
    messages, messages_by_ticker = get_all_news(sp500_companies)

    if messages:
        produce_to_kafka(messages)

        logging.info(f"Pipeline complete: {len(messages)} articles processed")
        logging.info(f"Files saved in data/ directory")
    else:
        logging.warning("No articles fetched")

def main_continuous():
    """
    Continuous mode: Run every 5 minutes
    Use for production deployment
    """
    while True:
        try:
            main()
            logging.info("Sleeping for 5 minutes...")
            time.sleep(300)
        except KeyboardInterrupt:
            logging.info("Shutting down...")
            break
        except Exception as e:
            logging.error(f"Error in main loop: {str(e)}")
            time.sleep(60)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    main()
    # main_continuous()
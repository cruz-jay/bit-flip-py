import json
import logging
import os
from datetime import datetime
from dotenv import load_dotenv
from supabase import create_client, Client
from quixstreams import Application

load_dotenv(".env")
url = os.getenv("SUPABASE_URL", "")
key = os.getenv("SUPABASE_KEY", "")
supabase: Client = create_client(url, key)

app = Application(
    broker_address='localhost:9092',
    loglevel="DEBUG",
    consumer_group='reddit-consumer-group',
    auto_offset_reset='latest',
)

def kafka_consumer():
    with app.get_consumer() as consumer:
        consumer.subscribe(topics=['reddit-wsb-posts-kafka'])

        while True:
            try:
                msg = consumer.poll(1)
                if msg is None:
                    logging.debug("No message")
                    continue
                elif msg.error():
                    logging.error(msg.error())
                    continue

                msg_key = msg.key().decode("utf-8") if msg.key() else "None"
                value = json.loads(msg.value().decode("utf-8"))
                offset = msg.offset()
                logging.debug(f"Received: key={msg_key}, value={value['id'][:10]}..., offset={offset}")

                supabase_consumer(value)  # Process
                consumer.store_offsets(msg)
                logging.info("Processed message")

            except KeyboardInterrupt:
                logging.info("Shutting down consumer")
                break
            except Exception as e:
                logging.error(f"Loop error: {e}")


def supabase_consumer(value):
    post_id = value.get("id")
    if not post_id:
        logging.error("No post_id in message")
        return

    exists = supabase.table("wallstreetbets_data").select("post_id").eq("post_id", post_id).execute()
    if exists.data:
        logging.info(f"Duplicate post_id {post_id} - skipping")
        return

    try:
        new_row_post = {
            "post_id": post_id,
            "title": " ".join(value.get("content", "").split()[:10]),
            "author": value.get("author"),
            "body": value.get("content"),
            "url": value.get("url"),
            "created_utc": value.get("created_utc"),
            "created": value.get("created"),
        }
        supabase.table("wallstreetbets_data").insert(new_row_post).execute()
        logging.info(f"Inserted post: {post_id}")

        mentions = value.get("ticker_mentions", {})
        for ticker, count in mentions.items():
            supabase.table("wallstreetbets_ticker").update({
                "total_mentions": supabase.table("wallstreetbets_ticker")
                                  .select("total_mentions")
                                  .eq("ticker", ticker)
                                  .execute().data[0]["total_mentions"] + count,
                "last_update": datetime.now().isoformat()
            }).eq("ticker", ticker).execute()

            logging.debug(f"Updated {ticker} count by +{count}")

    except Exception as e:
        logging.error(f"Supabase error for {post_id}: {e}")

if __name__ == '__main__':
    kafka_consumer()


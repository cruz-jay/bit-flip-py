import logging
import os
import csv
from datetime import datetime
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()
url = os.getenv("SUPABASE_URL", "")
key = os.getenv("SUPABASE_KEY", "")
supabase: Client = create_client(url, key)

csv_file_path = "../constituents.csv"
table_name = "wallstreetbets_ticker"

def main():
    with open(csv_file_path, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        data_to_insert = []

        for row in reader:
            ticker = row['Symbol'].strip().upper()
            company = row['Security'].strip()

            if ticker.isalpha() and 1 <= len(ticker) <= 5:
                data_to_insert.append({
                    'company': company,
                    'ticker': ticker,
                    'total_mentions': 0,
                    'last_update': datetime.now().isoformat()
                })

        if data_to_insert:
            response = supabase.table(table_name).insert(data_to_insert).execute()
            logging.info(f"Inserted {len(data_to_insert)} rows in {table_name} table.")
            logging.info(response)
        else:
            logging.info("No data inserted.")

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
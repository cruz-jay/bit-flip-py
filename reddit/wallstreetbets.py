import os
import pandas as pd
from dotenv import load_dotenv
import requests
import re
import datetime
import csv
import sys
from supabase import create_client, Client

load_dotenv(".env")
url = os.getenv("SUPABASE_URL", "")
key = os.getenv("SUPABASE_KEY", "")

supabase: Client = create_client(url, key)
reddit_url = "https://www.reddit.com/r/wallstreetbets/search/?q="




# os.getenv
def clean_company_name(company_name):
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

def load_security(csv_path="constituents.csv"):
    df = pd.read_csv(csv_path)

    data = []
    for _, row in df.iterrows():
        original_name = row["Security"]
        clean_name = clean_company_name(original_name)
        data.append([original_name, clean_name])

    flat_list = [item for sublist in data for item in sublist]

    return flat_list

def insert_into_db(flat_list):
    try:
        first = flat_list[0]
        # tickers = dict(ticker=first)
        new_row = {
            "ticker_matches": first,
            "author": "unknown",
            "title": "N/A",
            "created_at": "2025-10-11T00:00:00Z"
        }
        print(new_row)
        supabase.table("wallstreetbets").insert(new_row).execute()
        return "Success"
    except Exception as e:
        print(f"Error inserting into database: {e}")
        return "Failed"

def show_db():
    return supabase.table("wallstreetbets").select("*").execute()


if __name__ == "__main__":
    tickers_and_companies = load_security()
    row_data = insert_into_db(tickers_and_companies)


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


def load_sp500_tickers(csv_path="constituents.csv"):
    """Load S&P 500 tickers from CSV file"""
    df = pd.read_csv(csv_path)
    tickers = df['Security'].tolist()
    logging.info(f"Loaded {len(tickers)} S&P 500 tickers")

    return tickers


print(load_sp500_tickers())



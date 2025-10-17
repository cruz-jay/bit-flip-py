import requests
import sys

from dotenv import load_dotenv
import os


load_dotenv(".env")
client = os.getenv("REDDIT_CLIENT", "")
secret = os.getenv("REDDIT_KEY", "")


# ===============================
# CONFIGURATION
# ===============================

CLIENT_ID = client
CLIENT_SECRET = secret
USERNAME = "u/SecondaryBroccoli"

SUBREDDIT = "wallstreetbets"
START_TIMESTAMP = 1759323749
END_TIMESTAMP = 1760533373
KEYWORD = "NVIDIA"

def get_access_token():
    auth = requests.auth.HTTPBasicAuth(CLIENT_ID, CLIENT_SECRET)

    data = {
        "grant_type": "client_credentials"
    }

    headers = {
        "User-Agent": f"MyRedditApp/0.0.1 ({USERNAME})"
    }

    response = requests.post(
        "https://www.reddit.com/api/v1/access_token",
        auth=auth,
        data=data,
        headers=headers
    )

    if response.status_code != 200:
        print("Failed to get token:", response.status_code, response.text)
        sys.exit(1)

    token_json = response.json()
    print(token_json)
    return token_json["access_token"]


def search_posts(access_token):
    url = f"https://oauth.reddit.com/r/{SUBREDDIT}/search"

    headers = {
        "Authorization": f"bearer {access_token}",
        "User-Agent": f"MyRedditApp/0.0.1 ({USERNAME})"
    }

    params = {
        "q": KEYWORD,
        "sort": "new",
        "limit": 100,
        "restrict_sr": "on",
        "after": START_TIMESTAMP,
        "before": END_TIMESTAMP,
        "syntax": "cloudsearch"
    }

    response = requests.get(url, headers=headers, params=params)

    print("Status:", response.status_code)
    print("Response:", response.json())  # Debug line

    if response.status_code != 200:
        print("Search failed:", response.status_code, response.text)
        sys.exit(1)

    data = response.json()
    children = data.get("data", {}).get("children", [])

    if not children:
        print("No posts found for this query.")
    else:
        print(f"Found {len(children)} posts:")
        for child in children:
            post = child["data"]
            title = post.get("title")
            created = post.get("created_utc")
            permalink = post.get("permalink")
            print(f"- {title} | {created} | https://reddit.com{permalink}")




if __name__ == "__main__":
    token = get_access_token()
    search_posts(token)

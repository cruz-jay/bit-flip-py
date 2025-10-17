import requests
import json


def get_news():

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
    ticker = "apple"
    response = requests.get("https://www.tiingo.com/news/",
                            headers={
                                "Content-Type": "application/json",
                                "Authorization": f"Token {'b2a7fc10ab577f083f20bd247ab95de2e55fce36'}"
                            },
                            params={
                                "tickers": "apple",
                                "source": domains,
                                "limit": 10,
                            }
                            )

    print("Status code:", response.status_code)
    print("Response text:", response.text)
    data = response.json()

    return data

def main():
    data = get_news()
    print(data)


if __name__ == "__main__":
    main()


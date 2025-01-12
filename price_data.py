import argparse
import logging
import multiprocessing as mp
import os
import sqlite3

import ccxt
import pandas as pd
import requests
from tqdm import tqdm

logging.basicConfig(level=logging.INFO)

url_1 = "https://www.cryptodatadownload.com/cdd/Binance_{ticker}_{interval}.csv"
url_2 = "https://data.binance.vision/data/spot/monthly/klines/{ticker}/{interval}/{ticker}-{interval}-{date}.zip"
interval = "1h"
date_range = (
    pd.date_range(start="2021-01", end="2024-12", freq="MS").strftime("%Y-%m").tolist()
)
failed_urls = []
top_n = 50


def get_token_metadata():
    logging.info("Fetching top 100 tokens by market cap")

    binance = ccxt.binance()
    tickers = binance.fetch_tickers()

    # Filter out only USDT pairs and sort by market cap
    usdt_pairs = {
        symbol: ticker for symbol, ticker in tickers.items() if symbol.endswith("/USDT")
    }
    sorted_pairs = sorted(
        usdt_pairs.items(), key=lambda item: item[1]["quoteVolume"], reverse=True
    )

    # Get the top n pairs
    top_n_pairs = sorted_pairs[:top_n]

    # Get the metadata for the top n tokens
    with sqlite3.connect("./data/data.db") as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS token_metadata (
                token_name TEXT,
                market_cap REAL
            )
            """
        )
        conn.commit()

        cursor.executemany(
            "INSERT INTO token_metadata (token_name, market_cap) VALUES (?, ?)",
            [
                (pair[1]["info"]["symbol"], pair[1]["quoteVolume"])
                for pair in top_n_pairs
            ],
        )

    return {pair[1]["info"]["symbol"]: pair[1]["quoteVolume"] for pair in top_n_pairs}


def get_price_df(url):
    logging.info(f"processing {url}")

    file_name = f"{url.split('/')[-1]}"

    # Download and extract the data if it doesn't exist
    if not os.path.exists(f"./data/{file_name}"):
        logging.info(f"Downloading data for {url}")

        response = requests.get(url)
        if response.status_code != 200:
            logging.warn(f"Failed to download data for {url}")
            failed_urls.append(url)
            return
        else:
            with open(f"./data/{file_name}", "wb") as f:
                f.write(response.content)
            logging.info(f"created {file_name} successfully")

    # unix,date,symbol,open,high,low,close,Volume BTC,Volume USDT,tradecount

    df = pd.read_csv(f"./data/{file_name}", skiprows=1, header=0).iloc[
        :, [0, 2, 3, 4, 5, 6, 7]
    ]
    df.columns = ["timestamp", "token", "open", "high", "low", "close", "volume"]

    df = df.astype({
        "timestamp": "datetime64[ms]",
        "token": "string",
        "open": "float64",
        "high": "float64",
        "low": "float64",
        "close": "float64",
        "volume": "float64",
    })

    logging.info(f"Data for {url} processed successfully")

    return df


if __name__ == "__main__":
    #
    parser = argparse.ArgumentParser(description="Download Binance data")
    parser.add_argument(
        "--interval",
        type=str,
        default="1h",
        choices=["1h", "1d"],
    )
    parser.add_argument(
        "--top_n",
        type=int,
        default=50,
    )

    args = parser.parse_args()
    interval = args.interval
    top_n = args.top_n

    print(f"Downloading data for -> interval: {interval} | top_n: {top_n}")

    # Get the top 50 tokens by market cap
    top_tokens_meta = get_token_metadata()
    df_top_tokens_meta = pd.DataFrame(
        list(top_tokens_meta.items()), columns=["Token", "Market Cap"]
    )

    # crossproduct of tokens and date range, arguments for get_price_df
    cross_product = [
        url_1.format(ticker=ticker, interval=interval)
        for ticker in df_top_tokens_meta["Token"]
    ]

    # Download data for all tokens and date ranges
    with mp.Pool(mp.cpu_count()) as pool:
        dfs = list(
            tqdm(pool.map(get_price_df, cross_product), total=len(cross_product))
        )

    dfs = [df for df in dfs if df is not None]

    logging.info(f"Failed to download data for {failed_urls} urls")

    # dfs = [get_price_df(cross_product[0])]

    with sqlite3.connect("./data/data.db") as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS token_prices (
                token TEXT,
                timestamp DATETIME,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume REAL
            )
            """
        )
        conn.commit()

        for df in dfs:
            df.to_sql("token_prices", conn, if_exists="append", index=False)

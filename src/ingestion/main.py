import os
import logging
import tempfile
from datetime import datetime, timedelta

import boto3
import pandas as pd
import yfinance as yf
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration from .env
# ---------------------------------------------------------------------------
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET_RAW = os.getenv("S3_BUCKET_RAW")

# Comma-separated list of B3 tickers, e.g. "PETR4.SA,VALE3.SA,^BVSP"
TICKERS_ENV = os.getenv("B3_TICKERS", "PETR4.SA,VALE3.SA,ITUB4.SA,^BVSP")
TICKERS: list[str] = [t.strip() for t in TICKERS_ENV.split(",") if t.strip()]

# How many days back to fetch (default: yesterday only for a daily run)
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "1"))


def validate_config() -> None:
    missing = [
        name
        for name, value in {
            "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
            "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
            "S3_BUCKET_RAW": S3_BUCKET_RAW,
        }.items()
        if not value
    ]
    if missing:
        raise EnvironmentError(
            f"Missing required environment variables: {', '.join(missing)}"
        )


def build_s3_key(ticker: str, partition_date: datetime) -> str:
    """
    Returns a Hive-style partitioned S3 key under the raw/ prefix.

    Example: raw/ticker=PETR4.SA/year=2024/month=01/day=15/data.parquet
    """
    safe_ticker = ticker.replace("^", "INDEX_")
    return (
        f"raw/"
        f"ticker={safe_ticker}/"
        f"year={partition_date.strftime('%Y')}/"
        f"month={partition_date.strftime('%m')}/"
        f"day={partition_date.strftime('%d')}/"
        f"data.parquet"
    )


def fetch_ticker_data(ticker: str, start: str, end: str) -> pd.DataFrame:
    """Downloads daily OHLCV data for a single ticker via yfinance."""
    logger.info("Fetching %s from %s to %s", ticker, start, end)
    df = yf.download(ticker, start=start, end=end, interval="1d", auto_adjust=True, progress=False)

    if df.empty:
        logger.warning("No data returned for ticker %s", ticker)
        return df

    df = df.reset_index()

    # Flatten MultiIndex columns produced by yfinance when downloading a single ticker
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [col[0] if col[1] == "" else col[0] for col in df.columns]

    df.columns = [str(c).strip() for c in df.columns]
    df["Date"] = pd.to_datetime(df["Date"]).astype("datetime64[us]")
    df["ticker"] = ticker
    df["ingested_at"] = datetime.utcnow().isoformat()

    return df


def upload_to_s3(local_path: str, s3_key: str, s3_client) -> None:
    logger.info("Uploading to s3://%s/%s", S3_BUCKET_RAW, s3_key)
    s3_client.upload_file(local_path, S3_BUCKET_RAW, s3_key)
    logger.info("Upload complete: %s", s3_key)


def run() -> None:
    validate_config()

    s3 = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )

    end_date = datetime.utcnow().date()
    start_date = end_date - timedelta(days=LOOKBACK_DAYS)

    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    partition_date = datetime.combine(start_date, datetime.min.time())

    success, failed = [], []

    for ticker in TICKERS:
        try:
            df = fetch_ticker_data(ticker, start=start_str, end=end_str)

            if df.empty:
                logger.warning("Skipping %s — empty dataset.", ticker)
                failed.append(ticker)
                continue

            s3_key = build_s3_key(ticker, partition_date)

            with tempfile.NamedTemporaryFile(suffix=".parquet", delete=True) as tmp:
                df.to_parquet(tmp.name, index=False, engine="pyarrow")
                upload_to_s3(tmp.name, s3_key, s3)

            success.append(ticker)

        except Exception as exc:
            logger.error("Failed to process ticker %s: %s", ticker, exc, exc_info=True)
            failed.append(ticker)

    logger.info("Ingestion summary — success: %s | failed: %s", success, failed)

    if failed:
        raise RuntimeError(f"Ingestion failed for: {failed}")


if __name__ == "__main__":
    run()

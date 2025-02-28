import io

from datetime import datetime, timedelta
import time
from google.cloud import storage
from zoneinfo import ZoneInfo

import pandas as pd

from .alpaca_main import AlpacaClient
from .alpaca_defs import get_config_from_env


def get_dates():
    # Get current date (without time)
    today = datetime.now(ZoneInfo("America/New_York")).replace(
        hour=0, minute=0, second=0, microsecond=0
    )

    # Define January 1st, 2016
    start_date = datetime(2016, 1, 1, tzinfo=ZoneInfo("America/New_York"))

    # Iterate from today backwards to Jan 1, 2016
    current_date = today
    while current_date >= start_date:
        # Create start and end times for this day
        day_start = current_date.replace(hour=8, minute=55)
        day_end = current_date.replace(hour=16, minute=5)

        yield day_start, day_end

        # Move to the previous day
        current_date -= timedelta(days=1)


def save_df_to_gcs(
    dataframe: pd.DataFrame, destination_blob_name, bucket_name="alpaca_intraday_data"
):
    # Convert DataFrame to CSV string
    csv_buffer = io.StringIO()
    dataframe.to_csv(csv_buffer, index=True)

    # Create a client and upload the file
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    # Upload from string buffer
    blob.upload_from_string(csv_buffer.getvalue(), content_type="text/csv")

    print(f"DataFrame saved to gs://{bucket_name}/{destination_blob_name}")


def get_historical_dates():

    alpaca_config = get_config_from_env()

    client = AlpacaClient(alpaca_config["key"], alpaca_config["secret"])

    symbols = ["SPY", "VOO"]

    try:
        dates_iterator = get_dates()
        while True:
            day_start, day_end = next(dates_iterator)
            df = client.get_trades(
                symbols,
                start=day_start,
                end=day_end,
            )
            date = day_start.date().isoformat()
            destination_blob_name = (
                "stocks/intraday_data/daily_trade_session_trades/"
                + f"trades_{date}.csv"
            )
            save_df_to_gcs(df, destination_blob_name)
    except StopIteration:
        print("Finished fetching historical data.")
        return


if __name__ == "__main__":
    get_historical_dates()

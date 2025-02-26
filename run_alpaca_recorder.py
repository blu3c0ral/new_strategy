import configparser

from recorders.alpaca_recorder import AlpacaSnapshotRecorder
from persistence.gcp_cloud_storage import GCSPersistence


def main(event, context):
    # Get config from file config.ini
    config = configparser.ConfigParser()

    try:
        config.read("config.ini")
    except configparser.Error as e:
        print(f"Error reading INI file: {e}")
        exit(1)

    pl = GCSPersistence(
        bucket_name="alpaca_intraday_data",
        gcs_prefix="stocks/intraday_data",
        filename="snapshots_logs",
        format="json",
        file_per_day=True,
    )

    alpaca_recorder = AlpacaSnapshotRecorder(
        stocks=["SPY", "VOO"],
        config=dict(config.items("alpaca_account")),
        persistences=[pl],
    )

    alpaca_recorder.run_once()

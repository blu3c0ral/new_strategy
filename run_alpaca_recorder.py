import os

from recorders.alpaca_recorder import AlpacaSnapshotRecorder
from persistence.gcp_cloud_storage import GCSPersistence


def main():
    config = {
        "key": os.environ.get("ALPACA_KEY"),
        "secret": os.environ.get("ALPACA_SECRET"),
    }

    pl = GCSPersistence(
        bucket_name="alpaca_intraday_data",
        gcs_prefix="stocks/intraday_data",
        filename="snapshots_logs",
        format="json",
        file_per_day=True,
    )

    alpaca_recorder = AlpacaSnapshotRecorder(
        stocks=["SPY", "VOO"],
        config=config,
        persistences=[pl],
    )

    alpaca_recorder.run()


if __name__ == "__main__":
    main()

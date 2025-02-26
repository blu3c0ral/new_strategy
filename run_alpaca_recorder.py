from google.cloud import secretmanager

from recorders.alpaca_recorder import AlpacaSnapshotRecorder
from persistence.gcp_cloud_storage import GCSPersistence

PROJECT_ID = 797853389585


def access_secret(project_id, secret_id, version_id="latest"):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")


def main():
    config = {
        "key": access_secret(PROJECT_ID, "ALPACA_KEY"),
        "secret": access_secret(PROJECT_ID, "ALPACA_SECRET"),
    }

    print(config["key"])

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

import datetime
from zoneinfo import ZoneInfo
from google.cloud import secretmanager

from recorders.alpaca_recorder import AlpacaTradesRecorder
from persistence.gcp_bigquery import BigQueryPersistence

PROJECT_ID = 797853389585


def access_secret(project_id, secret_id, version_id="1"):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")


def main():
    config = {
        "key": access_secret(PROJECT_ID, "ALPACA_KEY"),
        "secret": access_secret(PROJECT_ID, "ALPACA_SECRET"),
    }

    pl = BigQueryPersistence(
        project_id="market-data-model-20",
        dataset_id="alpaca_dataset",
        table_id="trades",
    )

    hours_in_day = datetime.time(0, 23, 0, tzinfo=ZoneInfo("America/New_York"))

    alpaca_recorder = AlpacaTradesRecorder(
        stocks=["SPY", "VOO", "VTI", "SCHB"],
        config=config,
        persistences=[pl],
        hours_in_day=[hours_in_day],
    )

    # alpaca_recorder.run()

    alpaca_recorder.run_once()


if __name__ == "__main__":
    main()

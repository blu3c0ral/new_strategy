import configparser
import os
from flask import Flask, request, jsonify

from recorders.alpaca_recorder import AlpacaSnapshotRecorder
from persistence.gcp_cloud_storage import GCSPersistence

app = Flask(__name__)


@app.route("/", methods=["GET", "POST"])
def handler():
    # Create a simple event and context objects to pass to main
    event = request.get_json() if request.is_json else {}
    context = {}

    # Call the main function with these parameters
    result = main(event, context)

    return jsonify({"status": "success", "result": result})


def main(event, context):
    # Get config from file config.ini
    config = configparser.ConfigParser()

    try:
        config.read("config.ini")
    except configparser.Error as e:
        print(f"Error reading INI file: {e}")
        return {"error": f"Error reading INI file: {str(e)}"}

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
    return {"message": "AlpacaSnapshotRecorder executed successfully"}


if __name__ == "__main__":
    # Get port from environment variable or default to 8080
    port = int(os.environ.get("PORT", 8080))
    # Run the Flask app
    app.run(host="0.0.0.0", port=port, debug=False)

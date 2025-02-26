# Use the official lightweight Python 3.12 image
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Copy requirements.txt first for better layer caching
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy all project files except venv and vscode
COPY brokerage_systems ./brokerage_systems/
COPY persistence ./persistence/
COPY recorders ./recorders/
COPY __init__.py .
COPY config.ini .
COPY definitions.py .
COPY intraday_prices.csv .
COPY run_alpaca_recorder.py .

# Expose the port
EXPOSE 8080

# Run the Flask app
CMD exec python run_alpaca_recorder.py
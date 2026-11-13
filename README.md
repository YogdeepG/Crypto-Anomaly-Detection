# Real-Time Cryptocurrency Market Anomaly Detection Using Apache Kafka

## About the project

This project is a small real-time monitoring system for the BTC/USDT cryptocurrency market. It receives live trade information from Binance, sends the data through Apache Kafka, checks each trade for unusual activity, and displays the results on a Streamlit dashboard.

I built this project to explore how a real-time data pipeline works in practice: collecting live data, moving it through Kafka, processing it, saving the results, and presenting useful information.

The system looks for two types of unusual activity:

- **Unusual price movements** — when the current BTC price moves noticeably away from its recent history.
- **Unusually large trade volumes** — when a trade quantity is much larger than the recent historical pattern.

The alerts are statistical warnings. They do not prove that a trade is fraudulent or that the market is being manipulated.

## 1. Data source

The project uses Binance's public BTC/USDT trade WebSocket:

`wss://data-stream.binance.vision/ws/btcusdt@trade`

This provides live trade messages and does not require a Binance API key. An internet connection is needed while the producer is running.

## 2. How the system works

The data moves through the project in the following order:

1. The **producer** connects to Binance and receives live BTC/USDT trades.
2. The producer sends each trade to the Kafka topic `crypto-trades`.
3. The **consumer** reads the messages, checks that the trade data is valid, and runs the anomaly detection logic.
4. The consumer saves processed trades and alerts in an SQLite database.
5. It also publishes processed messages to `crypto-processed` and detected alerts to `crypto-alerts`.
6. The **Streamlit dashboard** reads the stored data and displays recent trades, prices, and alerts.

### Architecture

```text
Binance BTC/USDT WebSocket
            |
            v
       Live Producer
            |
            v
      Kafka: crypto-trades
            |
            v
         Consumer
       /    |     \
      /     |      \
     v      v       v
  SQLite  Kafka    Kafka
  trades  crypto-  crypto-
  alerts  processed alerts
      \     |      /
       \    |     /
            v
     Streamlit Dashboard
```

Kafka runs locally in Docker using KRaft mode, so a separate ZooKeeper service is not needed.

The project uses these Kafka topics:

| Topic | What it is used for |
|---|---|
| `crypto-trades` | Incoming trades from Binance |
| `crypto-processed` | Trades after validation and processing |
| `crypto-alerts` | Trades that triggered an anomaly alert |

The local Kafka setup is configured with three partitions per topic.

## 3. Tools and technologies

- **Python** — used to build the producer, consumer, anomaly detection logic, database functions, and performance monitor.
- **Apache Kafka** — moves trade messages through the pipeline.
- **Docker Compose** — runs Kafka locally.
- **Binance WebSocket** — provides the live market data.
- **SQLite** — stores processed trades and anomaly alerts.
- **Streamlit** — displays the dashboard.
- **pytest** — runs the automated tests.

## 4. Project folder structure

The main files are arranged like this:

```text
Crypto-Anomaly-Detection/
├── producer/
│   └── live_producer.py
├── consumer/
│   └── anomaly_detector.py
├── dashboard/
│   └── app.py
├── database/
│   └── database.py
├── metrics/
│   └── performance.py
├── tests/
│   ├── test_producer.py
│   ├── test_detector.py
│   └── test_pipeline.py
├── data/
├── config/
│   └── settings.py
├── docker-compose.yml
├── requirements.txt
├── .env.example
├── .gitignore
├── README.md
└── AI_PROMPTS.txt
```

There may be additional local files depending on the development environment.

## 5. What is needed to run it?

You will need:

- Python 3.10 or newer. The project was tested in the development environment with Python 3.13.13.
- Docker Desktop with Docker Compose.
- An internet connection for the Binance WebSocket feed.
- A terminal or an editor such as VS Code.

## 6. Installation

Run the following commands from the main `Crypto-Anomaly-Detection` project folder.

### Step 1: Create a virtual environment

On macOS or Linux:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

### Step 2: Install the required packages

```bash
pip install -r requirements.txt
```

### Step 3: Start Kafka

Make sure Docker Desktop is open, then run:

```bash
docker compose up -d
```

To check that the Kafka container is running:

```bash
docker compose ps
```

The Python applications connect to Kafka at `localhost:9092`.

## 7. Running the project

Keep each part of the application running in its own terminal. Open four terminals in VS Code, with the project folder as the working directory.

### Terminal 1 — Kafka

```bash
docker compose up -d
```

### Terminal 2 — Producer

```bash
source .venv/bin/activate
python producer/live_producer.py
```

The producer connects to Binance and sends incoming BTC/USDT trades to Kafka.

### Terminal 3 — Consumer

```bash
source .venv/bin/activate
python consumer/anomaly_detector.py
```

The consumer reads the trades from Kafka, validates them, checks for anomalies, saves the results in SQLite, and publishes the processed messages and alerts to Kafka.

### Terminal 4 — Dashboard

```bash
source .venv/bin/activate
streamlit run dashboard/app.py
```

Once Streamlit starts, open this address in your browser:

`http://localhost:8501`

The dashboard shows recent trade activity, the latest BTC price, anomaly alerts, and a BTC/USDT price chart. The dashboard refreshes periodically and displays statistics for a rolling five-minute window.

## 8. How anomaly detection works

The consumer checks each valid trade against recent trade history. It currently looks for price changes and unusually large trade quantities.

### Price movement

The detector compares the current price with the median of the recent price history. It calculates the percentage difference and a robust z-score using the median absolute deviation (MAD).

A price alert is raised when both configured conditions are exceeded:

- Price history: 20 observations.
- Percentage deviation threshold: 2%.
- Robust z-score threshold: 3.5.

### Trade volume

Trade quantities can vary significantly, so the detector first applies a logarithmic transformation. It then compares the current transformed quantity with the recent history using the median and MAD.

A volume alert is raised when the robust z-score is greater than 6.

The current settings are:

- Volume history: 500 observations.
- Robust z-score threshold: 6.

When a trade is identified as anomalous, it is not added to the normal reference history. This helps prevent unusual trades from immediately changing the baseline used for later comparisons.

### Anomaly score

The consumer calculates a score between 0 and 100 and assigns a severity:

| Score | Severity |
|---:|---|
| 75–100 | High |
| 50–74.99 | Medium |
| Greater than 0 and below 50 | Low |
| 0 | None |

These are the thresholds currently used in `consumer/anomaly_detector.py`. They can be adjusted there.

## 9. Database and dashboard

SQLite is used to keep a local record of processed trades and detected alerts. The database operations are handled in `database/database.py`.

The dashboard uses this information to show:

- The number of trades in the displayed time window.
- The number of anomaly alerts in that window.
- The latest BTC price.
- A BTC/USDT price chart with anomaly markers.
- A table of recent alerts.

The counters shown on the dashboard are based on its rolling five-minute window. They should not be interpreted as lifetime totals.

## 10. Tests

The project includes tests for the producer, anomaly detection, and pipeline behavior.

Run them from the project folder:

```bash
python -m pytest tests/ -v
```

During development, the test suite completed with **5 tests passed**. Run the tests again in your own environment before submitting, as results can vary after code changes.

You can also check the consumer file for Python syntax errors with:

```bash
python -m py_compile consumer/anomaly_detector.py
```

## 11. Performance results

During one test run, the consumer printed the following performance report:

| Measurement | Result |
|---|---:|
| Trades processed | 30,855 |
| Throughput | 64.09 trades per second |
| Average measured latency | 14,626.14 ms |
| Median measured latency | 9,760 ms |
| 95th percentile latency (P95) | 33,756 ms |

These figures are from one observed run on the development machine. They are not guaranteed results for other machines or workloads.

**A note about latency:** the performance monitor subtracts the producer's `ingestion_timestamp` from the consumer's `processing_timestamp`. Both timestamps are recorded in milliseconds. This gives an end-to-end time difference, which can include time spent waiting in Kafka. It is not just the time taken by the anomaly detection calculation.

For a more complete performance study, the test should be repeated under controlled conditions and should record the machine setup, test duration, and workload.

## 12. Basic reliability check

I also performed a basic consumer-restart check:

1. Kafka and the producer were left running.
2. The consumer was stopped.
3. The consumer was started again.
4. It reconnected to Kafka and resumed processing trades.

This checks recovery after restarting the consumer process. It does not prove that the system can withstand a Kafka broker failure, a machine failure, or every possible data-loss scenario.

## 13. Current limitations

There are a few things to keep in mind when interpreting the results:

- The current implementation monitors BTC/USDT only.
- The detector uses statistical thresholds rather than a trained machine-learning model.
- The alerts have not been compared with a labelled ground-truth dataset. Accuracy, precision, recall, and F1-score therefore have not been established.
- An alert means that a trade looks unusual according to the configured rules; it does not prove wrongdoing or market manipulation.
- The performance figures come from one run and need repeated, controlled testing before drawing wider conclusions.
- The local Kafka deployment uses a single broker. It is suitable for a project demonstration but does not provide production-level high availability.
- The latency measurement includes possible Kafka waiting time, not only the consumer's calculation time.

## 14. Stopping the application

Stop the producer, consumer, and dashboard separately. In each terminal, click inside the terminal and press `Control + C`.

When you are finished, stop Kafka with:

```bash
docker compose down
```

This stops the containers but keeps the named Kafka volume.

If you intentionally want to remove the persisted Kafka data as well, use:

```bash
docker compose down -v
```

Be careful with the second command: it deletes the Kafka data stored in the Docker volume.

## 15. Running it again

For another run:

1. Open the project folder.
2. Activate the virtual environment.
3. Start Docker Desktop.
4. Run `docker compose up -d`.
5. Start the producer, consumer, and dashboard in separate terminals.
6. Open `http://localhost:8501` to view the dashboard.
7. Run `python -m pytest tests/ -v` to check the tests.

The producer needs an internet connection to receive live data from Binance.

## Conclusion

This project brought together several parts of a real-time data system. It takes live BTC/USDT trades from Binance, sends them through Kafka, checks them for unusual price or volume activity, stores the results in SQLite, and displays them in a Streamlit dashboard.

The implementation and tests show that the main pipeline is working in the local development environment. The performance and restart checks provide an initial look at how it behaves. There is still room to improve the evaluation, especially by testing with labelled data, repeating performance tests under controlled conditions, and exploring a more fault-tolerant Kafka setup.

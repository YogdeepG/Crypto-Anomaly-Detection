import sqlite3
import json

from pathlib import Path


# Database Configuration
DATABASE_PATH = (
    Path(__file__).resolve().parent.parent
    / "data"
    / "crypto_trades.db"
)


def get_connection():
    # Create the data directory if it does not exist
    DATABASE_PATH.parent.mkdir(parents=True, exist_ok=True)

    # Connect to the SQLite database
    connection = sqlite3.connect(DATABASE_PATH)

    return connection


def create_tables():
    connection = get_connection()
    cursor = connection.cursor()

    # Table for all processed cryptocurrency trades
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            event_id INTEGER PRIMARY KEY,
            symbol TEXT,
            price REAL,
            quantity REAL,
            exchange_timestamp INTEGER,
            ingestion_timestamp INTEGER,
            processing_timestamp INTEGER,
            is_anomaly BOOLEAN,
            anomalies TEXT
        )
    """)

    # Table for detected anomalies
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id INTEGER,
            symbol TEXT,
            price REAL,
            quantity REAL,
            anomalies TEXT,
            alert_timestamp INTEGER
        )
    """)

    connection.commit()
    connection.close()


def save_trade(trade):
    connection = get_connection()
    cursor = connection.cursor()

    cursor.execute("""
        INSERT OR REPLACE INTO trades (
            event_id, symbol, price, quantity,
            exchange_timestamp, ingestion_timestamp,
            processing_timestamp, is_anomaly, anomalies
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        trade["event_id"],
        trade["symbol"],
        trade["price"],
        trade["quantity"],
        trade["exchange_timestamp"],
        trade["ingestion_timestamp"],
        trade["processing_timestamp"],
        trade["is_anomaly"],
        json.dumps(trade["anomalies"])
    ))

    connection.commit()
    connection.close()

    
def save_alert(trade):
    connection = get_connection()
    cursor = connection.cursor()

    cursor.execute("""
        INSERT INTO alerts (
            event_id, symbol, price, quantity,
            anomalies, alert_timestamp
        )
        VALUES (?, ?, ?, ?, ?, ?)
    """, (
        trade["event_id"],
        trade["symbol"],
        trade["price"],
        trade["quantity"],
        json.dumps(trade["anomalies"]),
        trade["processing_timestamp"]
    ))

    connection.commit()
    connection.close()        
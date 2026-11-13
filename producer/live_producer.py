
import json
import time
import ssl
import logging
import certifi

from datetime import datetime, timezone
from websocket import create_connection
from kafka import KafkaProducer

# Configuration
KAFKA_SERVER = "localhost:9092"
KAFKA_TOPIC = "crypto-trades"

BINANCE_URL = (
    "wss://data-stream.binance.vision/ws/btcusdt@trade"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def create_kafka_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",
        retries=5
    )


def start_stream():
    producer = create_kafka_producer()

    logging.info("Kafka producer connected successfully")

    while True:
        ws = None

        try:
            ws = create_connection(
                BINANCE_URL,
                timeout=30,
                sslopt={
                    "cert_reqs": ssl.CERT_REQUIRED,
                    "ca_certs": certifi.where()
                }
            )

            logging.info("Connected to Binance live stream")

            while True:
                message = ws.recv()

                if not message:
                    raise ConnectionError("WebSocket disconnected")

                trade = json.loads(message)

                if trade.get("e") != "trade":
                    continue

                event = {
                    "event_id": trade["t"],
                    "symbol": trade["s"],
                    "price": float(trade["p"]),
                    "quantity": float(trade["q"]),
                    "exchange_timestamp": trade["T"],
                    "ingestion_timestamp": time.time_ns() // 1_000_000,
                    "source": "binance"
                }

                producer.send(
                    KAFKA_TOPIC,
                    key=event["symbol"],
                    value=event
                ).get(timeout=10)

                print(
                    f"BTC Trade | "
                    f"Price: {event['price']} | "
                    f"Quantity: {event['quantity']}"
                )

        except KeyboardInterrupt:
            logging.info("Producer stopped by user")
            break

        except Exception as error:
            logging.error(f"Connection error: {error}")
            logging.info("Reconnecting in 5 seconds...")
            time.sleep(5)

        finally:
            if ws is not None:
                ws.close()

    producer.flush()
    producer.close()


if __name__ == "__main__":
    start_stream()

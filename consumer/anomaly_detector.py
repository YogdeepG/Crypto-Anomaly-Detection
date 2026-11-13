import json
import logging
import math
import signal
import sys
import time
from collections import deque
from pathlib import Path
from statistics import median

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from kafka import KafkaConsumer, KafkaProducer
from kafka.structs import OffsetAndMetadata, TopicPartition

from database.database import create_tables, save_trade, save_alert
from metrics.performance import PerformanceMonitor


# ============================================================
# KAFKA CONFIGURATION
# ============================================================

KAFKA_SERVER = "localhost:9092"

INPUT_TOPIC = "crypto-trades"
PROCESSED_TOPIC = "crypto-processed"
ALERT_TOPIC = "crypto-alerts"

CONSUMER_GROUP = "crypto-anomaly-detector"

# We commit offsets manually only after processing a message.
ENABLE_AUTO_COMMIT = False

performance_monitor = PerformanceMonitor()
shutdown_requested = False


# ============================================================
# LOGGING CONFIGURATION
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


# ============================================================
# KAFKA CONSUMER
# ============================================================

def create_kafka_consumer():
    """Create the Kafka consumer for incoming cryptocurrency trades."""
    return KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=KAFKA_SERVER,
        group_id=CONSUMER_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=ENABLE_AUTO_COMMIT,
        value_deserializer=lambda message: json.loads(
            message.decode("utf-8")
        ),
        max_poll_records=100,
        max_poll_interval_ms=300000
    )


# ============================================================
# ANOMALY DETECTION CONFIGURATION
# ============================================================

PRICE_HISTORY_SIZE = 20
PRICE_DEVIATION_THRESHOLD = 2.0
PRICE_ZSCORE_THRESHOLD = 3.5

VOLUME_HISTORY_SIZE = 500
VOLUME_ZSCORE_THRESHOLD = 6.0

recent_prices = deque(maxlen=PRICE_HISTORY_SIZE)
recent_volumes = deque(maxlen=VOLUME_HISTORY_SIZE)


# ============================================================
# ANOMALY SCORING
# ============================================================

def calculate_severity(score):
    """Convert a numerical anomaly score into a severity label."""
    if score >= 75:
        return "High"
    elif score >= 50:
        return "Medium"
    elif score > 0:
        return "Low"
    return "None"


def calculate_anomaly_score(
    price_deviation,
    volume_zscore,
    price_anomaly,
    volume_anomaly
):
    """
    Calculate a 0-100 anomaly score.

    The volume score uses the exact robust z-score calculated during
    detection. It does not reconstruct the z-score from a volume ratio.
    """
    scores = []

    if price_anomaly:
        price_score = min(
            100.0,
            (price_deviation / PRICE_DEVIATION_THRESHOLD) * 50
        )
        scores.append(price_score)

    if volume_anomaly:
        volume_score = min(
            100.0,
            (volume_zscore / VOLUME_ZSCORE_THRESHOLD) * 50
        )
        scores.append(volume_score)

    if not scores:
        return 0.0

    return round(max(scores), 2)


# ============================================================
# ANOMALY DETECTION
# ============================================================

def detect_anomaly_with_metrics(trade):
    """
    Detect price and volume anomalies.

    Returns:
        anomalies: list of detected anomaly descriptions
        price_deviation: percentage deviation from the historical median
        volume_zscore: robust z-score of log-transformed trade quantity
    """
    price = float(trade["price"])
    quantity = float(trade["quantity"])

    anomalies = []
    price_is_anomaly = False
    volume_is_anomaly = False

    price_deviation = 0.0
    volume_zscore = 0.0

    # --------------------------------------------------------
    # 1. PRICE ANOMALY DETECTION
    # --------------------------------------------------------

    if len(recent_prices) >= PRICE_HISTORY_SIZE:
        reference_price = median(recent_prices)

        if reference_price > 0:
            price_deviation = (
                abs(price - reference_price) / reference_price
            ) * 100

            price_deviations = [
                abs(value - reference_price)
                for value in recent_prices
            ]
            price_mad = median(price_deviations)

            if price_mad > 0:
                price_robust_zscore = (
                    abs(price - reference_price) / (1.4826 * price_mad)
                )
            else:
                price_robust_zscore = (
                    float("inf") if price != reference_price else 0.0
                )

            if (
                price_deviation > PRICE_DEVIATION_THRESHOLD
                and price_robust_zscore > PRICE_ZSCORE_THRESHOLD
            ):
                anomalies.append("Unusual Price Movement")
                price_is_anomaly = True

    # --------------------------------------------------------
    # 2. LOGARITHMIC VOLUME ANOMALY DETECTION
    # --------------------------------------------------------

    if len(recent_volumes) >= VOLUME_HISTORY_SIZE:
        # Log transformation reduces the influence of extreme values.
        log_volumes = [
            math.log1p(volume)
            for volume in recent_volumes
        ]

        current_log_volume = math.log1p(quantity)
        median_log_volume = median(log_volumes)

        deviations = [
            abs(value - median_log_volume)
            for value in log_volumes
        ]
        volume_mad = median(deviations)

        # Convert MAD into a robust standard-deviation estimate.
        robust_scale = 1.4826 * volume_mad

        if robust_scale > 0:
            volume_zscore = (
                current_log_volume - median_log_volume
            ) / robust_scale
        else:
            volume_zscore = (
                float("inf")
                if current_log_volume > median_log_volume
                else 0.0
            )

        if volume_zscore > VOLUME_ZSCORE_THRESHOLD:
            anomalies.append("Unusually Large Trade Volume")
            volume_is_anomaly = True

    # --------------------------------------------------------
    # 3. UPDATE HISTORICAL DATA
    # --------------------------------------------------------

    # Do not let detected anomalies distort the normal baseline.
    if not price_is_anomaly:
        recent_prices.append(price)

    if not volume_is_anomaly:
        recent_volumes.append(quantity)

    return anomalies, price_deviation, volume_zscore


def detect_anomaly(trade):
    """
    Compatibility wrapper that returns only the anomaly list.

    This keeps existing tests or other modules that expect
    detect_anomaly(trade) to return a list working as before.
    """
    anomalies, _, _ = detect_anomaly_with_metrics(trade)
    return anomalies


# ============================================================
# TRADE VALIDATION
# ============================================================

def validate_trade(trade):
    """Check that an incoming trade has all required valid fields."""
    if not isinstance(trade, dict):
        return False

    required_fields = [
        "event_id",
        "symbol",
        "price",
        "quantity",
        "ingestion_timestamp"
    ]

    if not all(field in trade for field in required_fields):
        return False

    try:
        price = float(trade["price"])
        quantity = float(trade["quantity"])
        ingestion_timestamp = float(trade["ingestion_timestamp"])
    except (TypeError, ValueError, OverflowError):
        return False

    if not all(
        math.isfinite(value)
        for value in [price, quantity, ingestion_timestamp]
    ):
        return False

    if price <= 0 or quantity <= 0:
        return False

    if not isinstance(trade["symbol"], str):
        return False

    if not trade["symbol"].strip():
        return False

    if not str(trade["event_id"]).strip():
        return False

    return True


# ============================================================
# PROCESS TRADE
# ============================================================

def process_trade(trade, producer):
    """Validate, detect, store, and publish one trade."""
    if not validate_trade(trade):
        logging.warning(
            "Invalid trade received. Skipping this message."
        )
        # This is a permanently invalid message, not a temporary
        # processing failure. Returning True allows its Kafka offset
        # to be committed so the consumer does not get stuck on it.
        return True

    trade = trade.copy()
    price = float(trade["price"])
    quantity = float(trade["quantity"])

    trade["price"] = price
    trade["quantity"] = quantity

    processing_start = time.perf_counter()

    # Detect anomalies and retain the exact measurements used.
    anomalies, price_deviation, volume_zscore = (
        detect_anomaly_with_metrics(trade)
    )

    is_anomaly = bool(anomalies)
    price_anomaly = "Unusual Price Movement" in anomalies
    volume_anomaly = "Unusually Large Trade Volume" in anomalies

    anomaly_score = calculate_anomaly_score(
        price_deviation,
        volume_zscore,
        price_anomaly,
        volume_anomaly
    )

    severity = calculate_severity(anomaly_score)

    # --------------------------------------------------------
    # PREPARE PROCESSED TRADE
    # --------------------------------------------------------

    processed_trade = trade.copy()
    processed_trade["anomalies"] = anomalies
    processed_trade["is_anomaly"] = is_anomaly
    processed_trade["anomaly_score"] = anomaly_score
    processed_trade["anomaly_severity"] = severity

    processed_trade["processing_timestamp"] = (
        time.time_ns() // 1_000_000
    )

    processing_duration_ms = (
        time.perf_counter() - processing_start
    ) * 1000

    processed_trade["detection_processing_ms"] = round(
        processing_duration_ms, 3
    )

    # --------------------------------------------------------
    # SAVE TRADE TO SQLITE
    # --------------------------------------------------------

    save_trade(processed_trade)

    # --------------------------------------------------------
    # RECORD PERFORMANCE
    # --------------------------------------------------------

    performance_monitor.record_trade(processed_trade)

    # --------------------------------------------------------
    # PUBLISH PROCESSED TRADE
    # --------------------------------------------------------

    producer.send(
        PROCESSED_TOPIC,
        key=trade["symbol"],
        value=processed_trade
    ).get(timeout=10)

    logging.info(
        "Processed Trade | Symbol: %s | Price: %.2f | "
        "Anomaly: %s | Score: %.2f | Severity: %s",
        trade["symbol"],
        price,
        is_anomaly,
        anomaly_score,
        severity
    )

    # --------------------------------------------------------
    # SAVE AND PUBLISH ALERT
    # --------------------------------------------------------

    if is_anomaly:
        alert = processed_trade.copy()

        save_alert(alert)

        producer.send(
            ALERT_TOPIC,
            key=trade["symbol"],
            value=alert
        ).get(timeout=10)

        logging.warning(
            "ANOMALY DETECTED | Symbol: %s | "
            "Price: %.2f | Quantity: %.6f | "
            "Score: %.2f | Severity: %s | Anomalies: %s",
            trade["symbol"],
            price,
            quantity,
            anomaly_score,
            severity,
            anomalies
        )

    return True


# ============================================================
# MANUAL KAFKA OFFSET COMMIT
# ============================================================

def commit_message(consumer, message):
    """Commit the offset after a message has been processed."""
    partition = TopicPartition(
        message.topic,
        message.partition
    )

    offsets = {
        partition: OffsetAndMetadata(
            message.offset + 1,
            ""
        )
    }

    consumer.commit(offsets=offsets)


# ============================================================
# GRACEFUL SHUTDOWN
# ============================================================

def request_shutdown(signum, frame):
    global shutdown_requested

    shutdown_requested = True
    logging.info(
        "Shutdown requested. Finishing current work..."
    )


# ============================================================
# MAIN FUNCTION
# ============================================================

def main():
    global shutdown_requested

    shutdown_requested = False

    signal.signal(signal.SIGINT, request_shutdown)
    signal.signal(signal.SIGTERM, request_shutdown)

    consumer = None
    producer = None

    try:
        create_tables()

        consumer = create_kafka_consumer()
        logging.info("Kafka consumer connected successfully")

        producer = KafkaProducer(
            bootstrap_servers=KAFKA_SERVER,
            value_serializer=lambda value: json.dumps(
                value
            ).encode("utf-8"),
            key_serializer=lambda key: key.encode("utf-8"),
            acks="all",
            retries=5
        )

        logging.info(
            "Kafka output producer connected successfully"
        )
        logging.info(
            "Waiting for cryptocurrency trade messages..."
        )

        # ----------------------------------------------------
        # CONTINUOUS TRADE PROCESSING
        # ----------------------------------------------------

        while not shutdown_requested:
            messages = consumer.poll(
                timeout_ms=1000,
                max_records=100
            )

            for partition_messages in messages.values():
                for message in partition_messages:
                    if shutdown_requested:
                        break

                    try:
                        success = process_trade(
                            message.value,
                            producer
                        )

                        if success:
                            # process_trade waits for each send with
                            # .get(timeout=10), so flushing after every
                            # message is unnecessary and slows throughput.
                            commit_message(consumer, message)

                    except Exception:
                        logging.exception(
                            "Trade processing failed. "
                            "Offset was not committed."
                        )

                        # Stop this partition batch after an error.
                        # The failed message can be replayed.
                        break

    except Exception:
        logging.exception(
            "An error occurred in the consumer"
        )

    finally:
        # ----------------------------------------------------
        # CLOSE KAFKA CONNECTIONS
        # ----------------------------------------------------

        if producer is not None:
            try:
                producer.flush()
                producer.close()
            except Exception:
                logging.exception(
                    "Producer shutdown error"
                )

        if consumer is not None:
            try:
                consumer.close()
            except Exception:
                logging.exception(
                    "Consumer shutdown error"
                )

        # ----------------------------------------------------
        # PERFORMANCE REPORT
        # ----------------------------------------------------

        try:
            report = performance_monitor.get_report()
            logging.info(
                "Performance report: %s",
                report
            )
        except Exception:
            logging.exception(
                "Could not generate performance report"
            )

        logging.info("Kafka connections closed")


# ============================================================
# APPLICATION ENTRY POINT
# ============================================================

if __name__ == "__main__":
    main()

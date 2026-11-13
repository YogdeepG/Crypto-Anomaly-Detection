
import consumer.anomaly_detector as detector


class FakeFuture:
    def get(self, timeout=None):
        return True


class FakeProducer:
    def __init__(self):
        self.messages = []

    def send(self, topic, key, value):
        self.messages.append({
            "topic": topic,
            "key": key,
            "value": value
        })
        return FakeFuture()


def prepare_history():
    detector.recent_prices.clear()
    detector.recent_volumes.clear()

    detector.recent_prices.extend(
        [68000.0] * detector.PRICE_HISTORY_SIZE
    )

    detector.recent_volumes.extend(
        [0.001] * detector.VOLUME_HISTORY_SIZE
    )


def create_trade(quantity=0.001):
    return {
        "event_id": 123456,
        "symbol": "BTCUSDT",
        "price": 68000.0,
        "quantity": quantity,
        "exchange_timestamp": 1794200016532,
        "ingestion_timestamp": 1794200016725,
        "source": "binance"
    }


def test_normal_trade_pipeline(monkeypatch):
    prepare_history()

    saved_trades = []
    saved_alerts = []

    monkeypatch.setattr(
        detector,
        "save_trade",
        lambda trade: saved_trades.append(trade)
    )

    monkeypatch.setattr(
        detector,
        "save_alert",
        lambda trade: saved_alerts.append(trade)
    )

    producer = FakeProducer()

    detector.process_trade(create_trade(), producer)

    assert len(saved_trades) == 1
    assert len(saved_alerts) == 0
    assert producer.messages[0]["topic"] == detector.PROCESSED_TOPIC


def test_anomaly_trade_pipeline(monkeypatch):
    prepare_history()

    saved_trades = []
    saved_alerts = []

    monkeypatch.setattr(
        detector,
        "save_trade",
        lambda trade: saved_trades.append(trade)
    )

    monkeypatch.setattr(
        detector,
        "save_alert",
        lambda trade: saved_alerts.append(trade)
    )

    producer = FakeProducer()

    detector.process_trade(
        create_trade(quantity=1.0),
        producer
    )

    assert len(saved_trades) == 1
    assert len(saved_alerts) == 1

    topics = [message["topic"] for message in producer.messages]

    assert detector.PROCESSED_TOPIC in topics
    assert detector.ALERT_TOPIC in topics

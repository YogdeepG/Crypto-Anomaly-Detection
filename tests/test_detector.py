from consumer import anomaly_detector as detector


def reset_history():
    detector.recent_prices.clear()
    detector.recent_volumes.clear()

    detector.recent_prices.extend(
        [68000] * detector.PRICE_HISTORY_SIZE
    )

    detector.recent_volumes.extend(
        [0.001] * detector.VOLUME_HISTORY_SIZE
    )


def test_no_anomaly():
    reset_history()

    trade = {
        "price": 68000,
        "quantity": 0.001
    }

    anomalies = detector.detect_anomaly(trade)

    assert anomalies == []


def test_detect_large_trade_volume():
    reset_history()

    trade = {
        "price": 68000,
        "quantity": 100
    }

    anomalies = detector.detect_anomaly(trade)

    assert "Unusually Large Trade Volume" in anomalies


def test_detect_price_deviation():
    reset_history()

    # Provide 20 normal historical prices
    for _ in range(20):
        detector.recent_prices.append(68000)

    trade = {
        "price": 70000,
        "quantity": 0.001
    }

    anomalies = detector.detect_anomaly(trade)

    assert "Unusual Price Movement" in anomalies

import time
import statistics
import math


class PerformanceMonitor:

    def __init__(self):
        self.processed_trades = 0
        self.latencies = []
        self.start_time = time.perf_counter()

    def record_trade(self, trade):
        """
        Record one processed trade and calculate its latency.
        Both timestamps must be in milliseconds.
        """

        self.processed_trades += 1

        try:
            ingestion_time = float(trade["ingestion_timestamp"])
            processing_time = float(trade["processing_timestamp"])

            latency_ms = processing_time - ingestion_time

            if math.isfinite(latency_ms) and latency_ms >= 0:
                self.latencies.append(latency_ms)

        except (KeyError, TypeError, ValueError):
            # Skip latency measurement if timestamps are invalid.
            pass

    def get_throughput(self):
        """
        Calculate processed trades per second.
        """

        elapsed_time = time.perf_counter() - self.start_time

        if elapsed_time <= 0:
            return 0.0

        return round(self.processed_trades / elapsed_time, 2)

    def get_latency_summary(self):
        """
        Calculate average, median and 95th percentile latency.
        """

        if not self.latencies:
            return {
                "average_ms": 0,
                "median_ms": 0,
                "p95_ms": 0
            }

        sorted_latencies = sorted(self.latencies)

        # Nearest-rank method for the 95th percentile.
        p95_index = max(
            0,
            math.ceil(len(sorted_latencies) * 0.95) - 1
        )

        return {
            "average_ms": round(
                statistics.mean(self.latencies), 2
            ),
            "median_ms": round(
                statistics.median(self.latencies), 2
            ),
            "p95_ms": round(
                sorted_latencies[p95_index], 2
            )
        }

    def get_report(self):
        """
        Return a complete performance report.
        """

        return {
            "total_processed_trades": self.processed_trades,
            "throughput_trades_per_second": self.get_throughput(),
            "latency": self.get_latency_summary()
        }

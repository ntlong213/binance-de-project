import json
import os
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic


TOPICS = {
    "trade":      os.getenv("KAFKA_TOPIC_TRADES",     "binance.trades"),
    "kline":      os.getenv("KAFKA_TOPIC_KLINES",     "binance.klines"),
    "bookTicker": os.getenv("KAFKA_TOPIC_BOOKTICKER", "binance.bookticker"),
}


def _create_topics(topics: list[str], num_partitions: int = 3, replication: int = 1):
    admin    = AdminClient({"bootstrap.servers": "localhost:29092"})
    existing = admin.list_topics(timeout=5).topics
    new      = [NewTopic(t, num_partitions, replication) for t in topics if t not in existing]
    if new:
        futures = admin.create_topics(new)
        for topic, future in futures.items():
            try:
                future.result()
                print(f"[Kafka] Topic created: {topic}")
            except Exception as e:
                print(f"[Kafka] Failed to create topic {topic}: {e}")


def delivery_report(err, _):
    if err:
        print(f"[Kafka] Delivery failed: {err}")


class KafkaProducer:
    def __init__(self):
        _create_topics(list(TOPICS.values()))
        self._producer = Producer({"bootstrap.servers": "localhost:29092"})
        print(f"[Kafka] Producer ready → localhost:29092")
        for k, v in TOPICS.items():
            print(f"         {k:12s} → {v}")

    def send(self, stream_type: str, symbol: str, data: dict):
        topic = TOPICS.get(stream_type)
        if not topic:
            return
        self._producer.produce(
            topic    = topic,
            key      = symbol.encode("utf-8"),
            value    = json.dumps(data).encode("utf-8"),
            callback = delivery_report,
        )
        self._producer.poll(0)

    def close(self):
        remaining = self._producer.flush(timeout=10)
        if remaining:
            print(f"[Kafka] {remaining} message(s) not delivered.")
        else:
            print("[Kafka] All messages delivered. Producer closed.")

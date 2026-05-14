import json
import os
import sys
from datetime import datetime
from pathlib import Path

from confluent_kafka import Consumer, KafkaException


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

from src.utils.config_loader import load_config


CONFIG_PATH = "src/config/pipeline_config.yml"


def create_kafka_consumer(topics: list) -> Consumer:
    """
    Create Kafka consumer and subscribe to topics from config.
    """

    consumer_config = {
        "bootstrap.servers": "localhost:9092",
        "group.id": "ecommerce_raw_json_consumer_group",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    }

    consumer = Consumer(consumer_config)
    consumer.subscribe(topics)

    print(f"Subscribed to topics: {topics}")

    return consumer


def get_topic_to_path_mapping(config: dict) -> dict:
    """
    Create mapping between Kafka topic and raw output path.

    Example:
    ecommerce.public.customers -> data/raw/customers
    """

    topic_to_path = {}

    sources = config.get("sources", {})

    for source_name, source_config in sources.items():
        if source_config.get("enabled", False):
            topic = source_config["kafka_topic"]
            raw_path = source_config["raw_path"]
            topic_to_path[topic] = raw_path

    return topic_to_path


def create_output_file_path(raw_path: str, topic: str) -> str:
    """
    Create a unique JSON file path for each Kafka message.
    """

    os.makedirs(raw_path, exist_ok=True)

    topic_name = topic.split(".")[-1]
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")

    file_name = f"{topic_name}_{timestamp}.json"

    return os.path.join(raw_path, file_name)


def write_message_to_json(raw_path: str, topic: str, message_value: dict) -> None:
    """
    Write one Kafka message to one raw JSON file.
    """

    output_file = create_output_file_path(raw_path, topic)

    with open(output_file, "w", encoding="utf-8") as file:
        json.dump(message_value, file, indent=4, default=str)

    print(f"Written message to: {output_file}")


def consume_messages() -> None:
    """
    Read Kafka messages and save them as raw JSON files.
    """

    config = load_config(CONFIG_PATH)

    topic_to_path = get_topic_to_path_mapping(config)

    if not topic_to_path:
        raise ValueError("No enabled Kafka sources found in config file")

    topics = list(topic_to_path.keys())

    consumer = create_kafka_consumer(topics)

    print("Kafka consumer started...")
    print("Press Ctrl + C to stop.")

    try:
        while True:
            message = consumer.poll(timeout=1.0)

            if message is None:
                continue

            if message.error():
                raise KafkaException(message.error())

            topic = message.topic()
            raw_path = topic_to_path.get(topic)

            if not raw_path:
                print(f"No raw path configured for topic: {topic}")
                continue

            message_value = message.value()

            if message_value is None:
                print(f"Skipping empty message from topic: {topic}")
                continue

            message_json = json.loads(message_value.decode("utf-8"))

            write_message_to_json(
                raw_path=raw_path,
                topic=topic,
                message_value=message_json,
            )

    except KeyboardInterrupt:
        print("Consumer stopped by user.")

    finally:
        consumer.close()
        print("Kafka consumer closed.")


if __name__ == "__main__":
    consume_messages()
import os
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer

_producer: AIOKafkaProducer | None = None


def _bootstrap_servers() -> str:
    return os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


def _common_kwargs() -> dict:
    kwargs: dict = {}
    security_protocol = os.environ.get("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
    kwargs["security_protocol"] = security_protocol

    if security_protocol in ("SASL_PLAINTEXT", "SASL_SSL"):
        kwargs["sasl_mechanism"] = os.environ.get("KAFKA_SASL_MECHANISM", "PLAIN")
        kwargs["sasl_plain_username"] = os.environ["KAFKA_SASL_USERNAME"]
        kwargs["sasl_plain_password"] = os.environ["KAFKA_SASL_PASSWORD"]

    return kwargs


async def get_producer() -> AIOKafkaProducer:
    global _producer
    if _producer is None:
        _producer = AIOKafkaProducer(
            bootstrap_servers=_bootstrap_servers(),
            **_common_kwargs(),
        )
        await _producer.start()
    return _producer


async def close_producer() -> None:
    global _producer
    if _producer is not None:
        await _producer.stop()
        _producer = None


def make_consumer(topics: list[str], group_id: str | None = None) -> AIOKafkaConsumer:
    return AIOKafkaConsumer(
        *topics,
        bootstrap_servers=_bootstrap_servers(),
        group_id=group_id or os.environ.get("KAFKA_CONSUMER_GROUP_ID", "main-api"),
        auto_offset_reset=os.environ.get("KAFKA_AUTO_OFFSET_RESET", "earliest"),
        **_common_kwargs(),
    )


async def ping() -> bool:
    producer = await get_producer()
    return producer.client is not None

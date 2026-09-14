#!/usr/bin/env python3
"""
Producer - Wikipedia Edit Stream Analytics
Connects to Wikimedia EventStreams SSE API and publishes events to Redpanda.
"""

import asyncio
import json
import logging
import os
import time
from datetime import datetime
from typing import Optional

import httpx
from confluent_kafka import Producer
from confluent_kafka import KafkaException
from pydantic import ValidationError

try:
    from producer.schema import WikimediaEvent, EventData, EventMeta
except ImportError:
    from schema import WikimediaEvent, EventData, EventMeta

# Logging configuration
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("producer")

# Configuration
REDPANDA_BOOTSTRAP_SERVERS = os.getenv("REDPANDA_BOOTSTRAP_SERVERS", "redpanda:9092")
REDPANDA_TOPIC = os.getenv("REDPANDA_TOPIC", "wikipedia.recentchange.raw")
SSE_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
USER_AGENT = "WikipediaEditStreamAnalytics/1.0 (Portfolio Project; https://github.com/example/wiki-stream)"

# Exponential backoff settings
BACKOFF_BASE = 1  # seconds
BACKOFF_MAX = 60  # seconds


class SSEClient:
    """Client untuk mengkonsumsi Server-Sent Events dari Wikimedia"""
    
    def __init__(self, url: str, user_agent: str):
        self.url = url
        self.user_agent = user_agent
        self.client: Optional[httpx.AsyncClient] = None
        self.response = None
        
    async def connect(self) -> bool:
        """Connect ke SSE stream dengan retry logic"""
        try:
            headers = {
                "User-Agent": self.user_agent,
                "Accept": "text/event-stream"
            }
            self.client = httpx.AsyncClient(timeout=None, headers=headers)
            # client.send() does not merge the client's default headers onto a
            # raw Request object (unlike client.get()/build_request()), so the
            # headers must be passed to the Request itself or Wikimedia sees
            # no User-Agent and returns 403 (Bug #29).
            self.response = await self.client.send(
                httpx.Request("GET", self.url, headers=headers),
                follow_redirects=True,
                stream=True
            )
            self.response.raise_for_status()
            logger.info(f"Connected to SSE stream: {self.url}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to SSE stream: {e}")
            await self._cleanup()
            return False
    
    async def _cleanup(self):
        """Clean up resources"""
        if self.client:
            await self.client.aclose()
            self.client = None
    
    async def read_events(self):
        """Generator untuk membaca event dari SSE stream"""
        buffer = ""
        async for line in self.response.aiter_lines():
            if not line:
                # Empty line indicates end of event
                if buffer:
                    yield buffer
                    buffer = ""
            else:
                buffer += line + "\n"
    
    async def disconnect(self):
        """Disconnect dari SSE stream"""
        if self.response is not None:
            await self.response.aclose()
            self.response = None
        await self._cleanup()
        logger.info("Disconnected from SSE stream")


class RedpandaProducer:
    """Producer untuk mengirim event ke Redpanda/Kafka"""
    
    def __init__(self, bootstrap_servers: str):
        self.bootstrap_servers = bootstrap_servers
        self.producer = None
        
    def _create_producer(self):
        """Create Kafka producer instance"""
        config = {
            "bootstrap.servers": self.bootstrap_servers,
            "client.id": "wikipedia-producer"
        }
        self.producer = Producer(config)
        
    def _delivery_callback(self, err, msg):
        """Callback untuk delivery report"""
        if err:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}]")
    
    def connect(self):
        """Connect ke Redpanda"""
        try:
            self._create_producer()
            logger.info(f"Connected to Redpanda at {self.bootstrap_servers}")
        except Exception as e:
            logger.error(f"Failed to connect to Redpanda: {e}")
            raise
    
    def publish(self, topic: str, key: str, value: dict) -> bool:
        """Publish message ke topic"""
        if not self.producer:
            self.connect()
        
        try:
            json_value = json.dumps(value, default=str).encode("utf-8")
            self.producer.produce(
                topic=topic,
                key=key.encode("utf-8") if key else None,
                value=json_value,
                callback=self._delivery_callback
            )
            self.producer.poll(0)
            return True
        except KafkaException as e:
            logger.error(f"Failed to publish message: {e}")
            return False
    
    def flush(self):
        """Flush all pending messages"""
        if self.producer:
            self.producer.flush()


def parse_sse_event(event_data: str) -> Optional[dict]:
    """Parse SSE event string menjadi dict."""
    lines = event_data.strip().split("\n")
    data_lines = []

    for line in lines:
        if line.startswith("data:"):
            data_lines.append(line[5:].strip())

    if not data_lines:
        return None

    if len(data_lines) == 1 and data_lines[0] == "":
        return {}

    data_str = "\n".join(data_lines)
    try:
        return json.loads(data_str)
    except json.JSONDecodeError as e:
        logger.warning(f"Failed to parse SSE event: {e}")
        if len(data_lines) > 1:
            return {}
        return None


def validate_event(event: dict) -> Optional[WikimediaEvent]:
    """Validate event menggunakan Pydantic schema"""
    try:
        # Wikimedia's recentchange SSE stream sends the change fields flat at
        # the top level (id, type, title, timestamp, ...) with only "meta"
        # nested — there is no separate "data" envelope like CloudEvents.
        meta = event.get("meta", {})
        data = {k: v for k, v in event.items() if k != "meta"}

        converted_event = {
            "type": event.get("type"),
            "topic": meta.get("topic", ""),
            "source": event.get("$schema", ""),
            "specversion": "1.0",
            "id": str(meta.get("id", event.get("id", ""))),
            "time": datetime.fromisoformat(meta.get("dt", "").replace("Z", "+00:00")),
        }

        # Normalize field types that Wikimedia sends as non-strings
        # Log-type changes (move/delete/protect/...) omit top-level "id" and
        # use "log_id" instead; fall back to the envelope's meta.id if even
        # that is absent, so every event has a usable, unique identifier.
        if not data.get("id"):
            data["id"] = data.get("log_id") or meta.get("id")
        if data.get("id") is not None:
            data["id"] = str(data["id"])
        if "timestamp" in data and data["timestamp"] is not None:
            data["timestamp"] = int(data["timestamp"])
        if "namespace" in data and data["namespace"] is not None:
            data["namespace"] = int(data["namespace"])
        if "minor" in data and data["minor"] is not None:
            data["minor"] = bool(data["minor"])
        for lk in ("length", "revision"):
            if lk in data and isinstance(data[lk], dict):
                for k in ("old", "new"):
                    if k in data[lk] and data[lk][k] is not None:
                        data[lk][k] = int(data[lk][k])

        # Parse data fields
        data_obj = EventData(**data)
        converted_event["data"] = data_obj

        # Parse meta fields
        meta_obj = EventMeta(**meta)
        converted_event["meta"] = meta_obj

        return WikimediaEvent(**converted_event)
    except ValidationError as e:
        logger.warning(f"Event validation failed: {e}")
        return None
    except Exception as e:
        logger.warning(f"Unexpected error during validation: {e}")
        return None


async def run_producer():
    """Main producer loop dengan exponential backoff reconnect"""
    sse_client = SSEClient(SSE_URL, USER_AGENT)
    redpanda = RedpandaProducer(REDPANDA_BOOTSTRAP_SERVERS)
    
    backoff = BACKOFF_BASE
    
    while True:
        try:
            # Connect to Redpanda
            redpanda.connect()
            
            # Connect to SSE stream
            if await sse_client.connect():
                backoff = BACKOFF_BASE  # Reset backoff on successful connection
                
                async for event_batch in sse_client.read_events():
                    if not event_batch:
                        continue
                    
                    # Parse and validate event
                    raw_event = parse_sse_event(event_batch)
                    if not raw_event:
                        continue
                    
                    validated_event = validate_event(raw_event)
                    if not validated_event:
                        continue
                    
                    # Publish to Redpanda
                    key = validated_event.data.id
                    value = validated_event.model_dump()
                    redpanda.publish(REDPANDA_TOPIC, key, value)
            else:
                # sse_client.connect() already logged the failure; raise so the
                # except block below applies exponential backoff before retrying.
                raise ConnectionError("Failed to connect to SSE stream")

        except asyncio.CancelledError:
            logger.info("Producer cancelled, shutting down...")
            break
        except Exception as e:
            logger.error(f"Producer error: {e}")
            
            # Disconnect clients
            await sse_client.disconnect()
            
            # Exponential backoff before reconnect
            logger.info(f"Reconnecting in {backoff} seconds...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, BACKOFF_MAX)
        finally:
            await sse_client.disconnect()


if __name__ == "__main__":
    try:
        asyncio.run(run_producer())
    except KeyboardInterrupt:
        logger.info("Producer stopped by user")

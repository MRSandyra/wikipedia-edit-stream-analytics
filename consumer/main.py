#!/usr/bin/env python3
"""
Consumer - Wikipedia Edit Stream Analytics
Konsumsi dari Redpanda, hitung suspicion score, dan tulis ke DuckDB + Parquet.
"""

import json
import logging
import os
import random
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional

from confluent_kafka import Consumer, KafkaException
from confluent_kafka import TopicPartition

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from scoring import calculate_suspicion_score, load_suspicious_keywords

# Logging configuration
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("consumer")

# Configuration
REDPANDA_BOOTSTRAP_SERVERS = os.getenv("REDPANDA_BOOTSTRAP_SERVERS", "localhost:9092")
REDPANDA_TOPIC = os.getenv("REDPANDA_TOPIC", "wikipedia.recentchange.raw")
GROUP_ID = "wikipedia-consumer"

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/app/data/wikipedia.duckdb")
BRONZE_PATH = os.getenv("BRONZE_PATH", "/app/data/bronze")

# Batch settings
BATCH_SIZE = 100
BATCH_FLUSH_INTERVAL = 60  # seconds


def connect_duckdb_with_retry(path, read_only=False, retries=5, base_delay=0.2):
    """Open a DuckDB connection, retrying with jittered backoff on lock
    conflicts. DuckDB's embedded file allows only one process to hold it at
    a time, and consumer/aggregator/dashboard all touch the same file
    periodically — brief collisions are expected, not exceptional (Bug #33).
    """
    for attempt in range(retries):
        try:
            return duckdb.connect(path, read_only=read_only)
        except Exception as e:
            if "lock" not in str(e).lower() or attempt == retries - 1:
                raise
            time.sleep(base_delay * (attempt + 1) + random.uniform(0, base_delay))


class WikipediaConsumer:
    """Consumer untuk Wikipedia edit stream"""
    
    def __init__(self, bootstrap_servers: str, topic: str, group_id: str):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.consumer = None
        self.suspicious_keywords = load_suspicious_keywords()
        self.running = False
        
        # Recent edits cache untuk rule 5 (user sama edit halaman sama >3 kali dalam 5 menit)
        self.recent_edits: List[Dict[str, Any]] = []

        # Silver (DuckDB) batch buffer. DuckDB's embedded file format only
        # allows one process to hold the file open at a time, so the
        # connection is opened only for the brief duration of a flush
        # instead of held for the consumer's whole lifetime — otherwise the
        # aggregator can never acquire the lock to build gold tables
        # (Bug #32).
        self.silver_buffer: List[Dict[str, Any]] = []
        self.last_silver_flush = time.time()

        # Parquet batch buffer
        self.bronze_buffer: List[Dict[str, Any]] = []
        self.last_bronze_flush = time.time()
        
        # Statistics
        self.total_processed = 0
        self.total_suspicious = 0
        self.batches_processed = 0
        
    def _create_consumer(self):
        """Create Kafka consumer"""
        config = {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": self.group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
            "auto.commit.interval.ms": 5000,
            "max.poll.interval.ms": 300000,
            "session.timeout.ms": 10000,
            "heartbeat.interval.ms": 3000
        }
        self.consumer = Consumer(config)
        
    def connect(self):
        """Connect ke Redpanda."""
        self._create_consumer()
        self.consumer.subscribe([self.topic])
        logger.info(f"Connected to Redpanda and subscribed to {self.topic}")

    def _init_duckdb_schema(self):
        """Create the enriched_edits table if needed, via a short-lived connection."""
        conn = connect_duckdb_with_retry(DUCKDB_PATH)
        try:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS enriched_edits (
                    id VARCHAR,
                    type VARCHAR,
                    namespace INTEGER,
                    title VARCHAR,
                    comment VARCHAR,
                    user VARCHAR,
                    bot BOOLEAN,
                    length_old INTEGER,
                    length_new INTEGER,
                    revision_old BIGINT,
                    revision_new BIGINT,
                    wiki VARCHAR,
                    server_name VARCHAR,
                    suspicion_score INTEGER,
                    is_suspicious BOOLEAN,
                    processed_at TIMESTAMP,
                    raw_event JSON,
                    PRIMARY KEY (id)
                )
            """)
            logger.info("DuckDB initialized with enriched_edits table")
        finally:
            conn.close()
    
    def _check_bronze_path(self):
        """Create bronze path if not exists"""
        path = Path(BRONZE_PATH)
        path.mkdir(parents=True, exist_ok=True)
    
    def process_event(self, raw_message: Dict[str, Any]) -> Dict[str, Any]:
        """Process single event: calculate score, determine if suspicious"""
        event_data = raw_message.get("data", {})
        
        # The consumer loop is synchronous; the cache is updated in this thread.
        recent = list(self.recent_edits)
        
        # Calculate suspicion score
        score = calculate_suspicion_score(event_data, recent, self.suspicious_keywords)
        is_suspicious = score >= 50
        
        # Build enriched record
        enriched = {
            "id": event_data.get("id"),
            "type": event_data.get("type"),
            "namespace": event_data.get("namespace", 0),
            "title": event_data.get("title"),
            "comment": event_data.get("comment"),
            "user": event_data.get("user"),
            "bot": event_data.get("bot", False),
            "length_old": event_data.get("length", {}).get("old", 0),
            "length_new": event_data.get("length", {}).get("new", 0),
            "revision_old": event_data.get("revision", {}).get("old", 0),
            "revision_new": event_data.get("revision", {}).get("new", 0),
            "wiki": event_data.get("wiki"),
            "server_name": event_data.get("server_name"),
            "suspicion_score": score,
            "is_suspicious": is_suspicious,
            "processed_at": datetime.now(),
            "raw_event": json.dumps(raw_message)
        }
        
        # Update recent edits cache (synchronous — single-threaded consumer loop)
        current_time = datetime.now()
        edit_copy = enriched.copy()
        edit_copy["processed_at"] = current_time
        self.recent_edits.append(edit_copy)
        
        # Prune old entries
        window_start = current_time - timedelta(minutes=5)
        self.recent_edits = [
            e for e in self.recent_edits
            if isinstance(e.get("processed_at"), datetime) and e["processed_at"] >= window_start
        ]
        # Keep bounded
        if len(self.recent_edits) > 1000:
            self.recent_edits = self.recent_edits[-1000:]
        
        # Update statistics
        self.total_processed += 1
        if is_suspicious:
            self.total_suspicious += 1
        
        return enriched
    
    def buffer_silver(self, record: Dict[str, Any]):
        """Buffer enriched record untuk batch write ke DuckDB silver table"""
        self.silver_buffer.append(record)

        current_time = time.time()
        should_flush = (
            len(self.silver_buffer) >= BATCH_SIZE or
            (current_time - self.last_silver_flush) >= BATCH_FLUSH_INTERVAL
        )
        if should_flush:
            self.flush_silver()

    def flush_silver(self):
        """Flush buffered records to DuckDB, holding the file lock only briefly"""
        if not self.silver_buffer:
            return

        try:
            conn = connect_duckdb_with_retry(DUCKDB_PATH)
            try:
                for record in self.silver_buffer:
                    conn.execute("""
                        INSERT INTO enriched_edits VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT (id) DO UPDATE SET
                            suspicion_score = excluded.suspicion_score,
                            is_suspicious = excluded.is_suspicious,
                            processed_at = excluded.processed_at,
                            raw_event = excluded.raw_event
                    """, [
                        record["id"], record["type"], record["namespace"], record["title"],
                        record["comment"], record["user"], record["bot"], record["length_old"],
                        record["length_new"], record["revision_old"], record["revision_new"],
                        record["wiki"], record["server_name"], record["suspicion_score"],
                        record["is_suspicious"], record["processed_at"], record["raw_event"]
                    ])
            finally:
                conn.close()
            logger.info(f"Flushed {len(self.silver_buffer)} records to DuckDB silver table")
            self.silver_buffer = []
            self.last_silver_flush = time.time()
        except Exception as e:
            logger.error(f"Failed to write to DuckDB: {e}")
    
    def buffer_bronze(self, raw_message: Dict[str, Any]):
        """Buffer raw message untuk batch write ke Parquet"""
        self.bronze_buffer.append(raw_message)
        
        # Check if should flush
        current_time = time.time()
        should_flush = (
            len(self.bronze_buffer) >= BATCH_SIZE or
            (current_time - self.last_bronze_flush) >= BATCH_FLUSH_INTERVAL
        )
        
        if should_flush:
            self.flush_bronze()
    
    def flush_bronze(self):
        """Flush buffered messages to Parquet files"""
        if not self.bronze_buffer:
            return
        
        try:
            self._check_bronze_path()
            
            # Group by date and hour
            partitions: Dict[str, List[Dict]] = {}
            for msg in self.bronze_buffer:
                dt = datetime.fromisoformat(msg.get("time", "").replace("Z", "+00:00"))
                partition_key = f"date={dt.strftime('%Y-%m-%d')}/hour={dt.strftime('%H')}"
                if partition_key not in partitions:
                    partitions[partition_key] = []
                partitions[partition_key].append(msg)
            
            # Write each partition
            for partition_key, messages in partitions.items():
                partition_path = Path(BRONZE_PATH) / partition_key
                partition_path.mkdir(parents=True, exist_ok=True)
                
                filename = f"batch_{int(time.time())}_{len(messages)}.parquet"
                filepath = partition_path / filename
                
                # Convert to PyArrow table
                if messages:
                    # Use first message to determine schema
                    sample = messages[0]
                    schema = pa.schema([
                        ("type", pa.string()),
                        ("topic", pa.string()),
                        ("source", pa.string()),
                        ("specversion", pa.string()),
                        ("id", pa.string()),
                        ("time", pa.timestamp("us")),
                        ("data", pa.string()),  # Keep as JSON string
                        ("meta", pa.string()),  # Keep as JSON string
                    ])
                    
                    rows = []
                    for msg in messages:
                        rows.append({
                            "type": msg.get("type"),
                            "topic": msg.get("topic"),
                            "source": msg.get("source", ""),
                            "specversion": msg.get("specversion", "1.0"),
                            "id": msg.get("id"),
                            "time": datetime.fromisoformat(msg.get("time", "").replace("Z", "+00:00")),
                            "data": json.dumps(msg.get("data", {})),
                            "meta": json.dumps(msg.get("meta", {}))
                        })
                    
                    table = pa.Table.from_pylist(rows, schema=schema)
                    pq.write_table(table, str(filepath))
            
            logger.info(f"Flushed {len(self.bronze_buffer)} messages to bronze storage")
            self.bronze_buffer = []
            self.last_bronze_flush = time.time()
            self.batches_processed += 1
            
        except Exception as e:
            logger.error(f"Failed to flush bronze: {e}")
    
    def run(self):
        """Main consumer loop"""
        self.connect()
        self._init_duckdb_schema()
        self.running = True

        logger.info("Starting consumer loop...")
        
        try:
            while self.running:
                message = self.consumer.poll(1.0)  # 1 second timeout
                
                if message is None:
                    continue
                
                if message.error():
                    logger.error(f"Consumer error: {message.error()}")
                    continue
                
                try:
                    raw_message = json.loads(message.value().decode("utf-8"))
                    
                    # Process event
                    enriched = self.process_event(raw_message)
                    
                    # Buffer for DuckDB (silver)
                    self.buffer_silver(enriched)
                    
                    # Buffer for bronze
                    self.buffer_bronze(raw_message)
                    
                    # Log periodically
                    if self.total_processed % 100 == 0:
                        logger.info(
                            f"Processed {self.total_processed} events, "
                            f"{self.total_suspicious} suspicious "
                            f"({self.total_suspicious/self.total_processed*100:.1f}%)"
                        )
                        
                except json.JSONDecodeError as e:
                    logger.warning(f"Failed to decode message: {e}")
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    
        except KeyboardInterrupt:
            logger.info("Consumer stopped by user")
        finally:
            self.shutdown()
    
    def shutdown(self):
        """Cleanup on shutdown"""
        self.running = False

        # Flush remaining buffers
        self.flush_silver()
        self.flush_bronze()

        if self.consumer:
            self.consumer.close()
            logger.info("Kafka consumer closed")


if __name__ == "__main__":
    consumer = WikipediaConsumer(REDPANDA_BOOTSTRAP_SERVERS, REDPANDA_TOPIC, GROUP_ID)
    try:
        consumer.run()
    except KeyboardInterrupt:
        logger.info("Consumer stopped by user")
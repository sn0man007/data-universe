import os
import logging
import threading
import psycopg2
from collections import defaultdict
from common import constants, utils
from common.data import (
    CompressedEntityBucket,
    CompressedMinerIndex,
    DataEntity,
    DataEntityBucket,
    DataEntityBucketId,
    DataLabel,
    DataSource,
    TimeBucket,
    HuggingFaceMetadata,
)
from storage.miner.miner_storage import MinerStorage
from typing import Dict, List
import datetime as dt

# Configure logging for this module
logger = logging.getLogger(__name__)

class PostgresMinerStorage(MinerStorage):
    """PostgreSQL backed MinerStorage."""

    def __init__(self):
        """Initializes the PostgreSQL miner storage."""
        # --- IMPORTANT: Replace with your actual database credentials ---
        self.dbname = "miner_db"
        self.user = "miner_user"
        self.password = "sn0wyhuh"
        self.host = "localhost"
        self.port = "5432"
        # --- End of credentials ---

        self.create_tables_if_not_exists()
        self.clearing_space_lock = threading.Lock()
        self.cached_index_refresh_lock = threading.Lock()
        self.cached_index_lock = threading.Lock()
        self.cached_index_4 = None
        self.cached_index_updated = dt.datetime.min

    def _create_connection(self):
        """Creates a new PostgreSQL connection."""
        try:
            conn_string = f"dbname='{self.dbname}' user='{self.user}' password='{self.password}' host='{self.host}' port='{self.port}'"
            return psycopg2.connect(conn_string)
        except psycopg2.OperationalError as e:
            logger.error(f"FATAL: Could not connect to PostgreSQL database. Please check credentials and server status. Error: {e}")
            raise

    def create_tables_if_not_exists(self):
        """Creates the necessary tables in the PostgreSQL database if they don't already exist."""
        with self._create_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS data_entities (
                    uri              TEXT PRIMARY KEY,
                    datetime         TIMESTAMPTZ NOT NULL,
                    time_bucket_id   BIGINT NOT NULL,
                    source           SMALLINT NOT NULL,
                    label            VARCHAR(255),
                    content          BYTEA NOT NULL,
                    content_size_bytes BIGINT NOT NULL
                );
                """)
                cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_data_entities_bucket
                ON data_entities (time_bucket_id, source, label);
                """)
                conn.commit()

    def store_data_entities(self, data_entities: List[DataEntity]):
        """Stores a list of DataEntity objects in the PostgreSQL database."""
        if not data_entities:
            return

        values_to_insert = []
        for data_entity in data_entities:
            label = data_entity.label.value if data_entity.label else None
            time_bucket_id = TimeBucket.from_datetime(data_entity.datetime).id
            values_to_insert.append((
                data_entity.uri,
                data_entity.datetime,
                time_bucket_id,
                data_entity.source,
                label,
                data_entity.content,
                data_entity.content_size_bytes
            ))

        with self._create_connection() as conn:
            with conn.cursor() as cursor:
                sql = """
                INSERT INTO data_entities (uri, datetime, time_bucket_id, source, label, content, content_size_bytes)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (uri) DO UPDATE SET
                    datetime = EXCLUDED.datetime,
                    content = EXCLUDED.content,
                    content_size_bytes = EXCLUDED.content_size_bytes;
                """
                cursor.executemany(sql, values_to_insert)
            conn.commit()

    def list_data_entities_in_data_entity_bucket(self, data_entity_bucket_id: DataEntityBucketId) -> List[DataEntity]:
        """Lists from storage all DataEntities matching the provided DataEntityBucketId."""
        data_entities = []
        label = data_entity_bucket_id.label.value if data_entity_bucket_id.label else None
        
        with self._create_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT * FROM data_entities WHERE time_bucket_id = %s AND source = %s AND label = %s",
                    (data_entity_bucket_id.time_bucket.id, data_entity_bucket_id.source.value, label)
                )
                for row in cursor.fetchall():
                    # Assuming row is a tuple: (uri, datetime, time_bucket_id, source, label, content, content_size_bytes)
                    data_entities.append(DataEntity(
                        uri=row[0], datetime=row[1], source=DataSource(row[3]),
                        label=DataLabel(value=row[4]) if row[4] else None,
                        content=row[5], content_size_bytes=row[6]
                    ))
        return data_entities

    def refresh_compressed_index(self, time_delta: dt.timedelta):
        """Refreshes the compressed MinerIndex."""
        # This is a complex method that interacts with the miner's core logic.
        # For the pipeline to function, a placeholder is sufficient for now.
        logger.info("refresh_compressed_index is a placeholder and has not been implemented for PostgreSQL yet.")
        pass

    def list_contents_in_data_entity_buckets(self, data_entity_bucket_ids: List[DataEntityBucketId]) -> Dict[DataEntityBucketId, List[bytes]]:
        logger.warning("list_contents_in_data_entity_buckets is not yet implemented for PostgreSQL.")
        return defaultdict(list)
        
    def get_compressed_index(self, bucket_count_limit=None) -> CompressedMinerIndex:
        logger.warning("get_compressed_index is not yet implemented for PostgreSQL.")
        return CompressedMinerIndex(sources={})

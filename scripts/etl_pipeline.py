import os
import sys
import json
import logging
import traceback
import datetime as dt
from apify_client import ApifyClient
from dotenv import load_dotenv

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)
dotenv_path = os.path.join(project_root, '.env')
load_dotenv(dotenv_path=dotenv_path)

from common.data import DataEntity, DataSource, DataLabel
from storage.miner.sqlite_miner_storage import PostgresMinerStorage

logging.basicConfig(level=logging.INFO, format='🚚 ETL: %(asctime)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

APIFY_TOKEN = os.getenv("APIFY_TOKEN")

def parse_datetime(datetime_str: str) -> dt.datetime:
    for fmt in ('%a %b %d %H:%M:%S %z %Y',):
        try:
            return dt.datetime.strptime(datetime_str, fmt)
        except (ValueError, TypeError):
            pass
    try:
        return dt.datetime.fromisoformat(str(datetime_str).replace('Z', '+00:00'))
    except (ValueError, TypeError):
        return dt.datetime.now(dt.timezone.utc)

def transform_twitter_item(item: dict, label: str) -> DataEntity | None:
    try:
        content_str = item.get("full_text") or item.get("text")
        url = item.get('url')
        datetime_str = item.get('created_at')
        if not all([content_str, url, datetime_str]): return None
        content = content_str.encode('utf-8')
        return DataEntity(uri=url, datetime=parse_datetime(datetime_str), source=DataSource.X, label=DataLabel(value=label), content=content, content_size_bytes=len(content))
    except Exception: return None

def transform_reddit_item(item: dict, label: str) -> DataEntity | None:
    try:
        content_text = f"{item.get('title', '')}\n\n{item.get('body', '')}".strip()
        url = item.get('url')
        datetime_str = item.get('createdAt')
        if not all([content_text, url, datetime_str]): return None
        content = content_text.encode('utf-8')
        return DataEntity(uri=url, datetime=parse_datetime(datetime_str), source=DataSource.REDDIT, label=DataLabel(value=label), content=content, content_size_bytes=len(content))
    except Exception: return None

def transform_youtube_item(item: dict, label: str) -> DataEntity | None:
    try:
        transcript = item.get('text') or item.get('transcript')
        url = item.get('url')
        if not all([transcript, url]): return None
        content = transcript.encode('utf-8')
        youtube_label = item.get('title') or label
        return DataEntity(uri=url, datetime=dt.datetime.now(dt.timezone.utc), source=DataSource.YOUTUBE, label=DataLabel(value=youtube_label), content=content, content_size_bytes=len(content))
    except Exception: return None

def run_etl_for_run(client: ApifyClient, storage: PostgresMinerStorage, run_id: str, transformer: callable, label: str) -> dt.datetime | None:
    logging.info(f"Fetching dataset items for run_id: {run_id}")
    dataset_items = client.run(run_id).dataset().list_items().items
    if not dataset_items:
        logging.warning(f"No items found for run_id: {run_id}.")
        return None

    valid_entities = [entity for item in dataset_items if (entity := transformer(item, label)) is not None]
    if not valid_entities:
        logging.warning(f"No valid entities were transformed from run_id: {run_id}.")
        return None

    logging.info(f"Storing {len(valid_entities)} DataEntities into the database...")
    storage.store_data_entities(valid_entities)
    logging.info(f"Successfully stored {len(valid_entities)} entities from run_id: {run_id}")

    latest_datetime = max(entity.datetime for entity in valid_entities)
    logging.info(f"Newest item in this batch has timestamp: {latest_datetime.isoformat()}")

    run_info = client.run(run_id).get()
    if run_info and 'defaultDatasetId' in run_info:
        dataset_id = run_info['defaultDatasetId']
        logging.info(f"Cleaning up Apify dataset {dataset_id} for run_id: {run_id}...")
        client.dataset(dataset_id).delete()
        logging.info(f"Successfully deleted Apify dataset {dataset_id}.")
    
    return latest_datetime

def main(run_infos: list, storage: PostgresMinerStorage) -> dict:
    latest_timestamps = {}
    if not APIFY_TOKEN:
        logging.error("APIFY_TOKEN is not set. Exiting.")
        return {}
    client = ApifyClient(APIFY_TOKEN)
    logging.info(f"--- Starting ETL Process for {len(run_infos)} successful runs ---")
    transformer_map = { DataSource.X: transform_twitter_item, DataSource.REDDIT: transform_reddit_item, DataSource.YOUTUBE: transform_youtube_item }
    for run_info in run_infos:
        transformer = transformer_map.get(run_info['source'])
        if transformer:
            newest_timestamp = run_etl_for_run(client=client, storage=storage, run_id=run_info['run_id'], transformer=transformer, label=run_info['label'])
            if newest_timestamp:
                label = run_info['label']
                if label not in latest_timestamps or newest_timestamp > latest_timestamps[label]:
                    latest_timestamps[label] = newest_timestamp
    logging.info("--- ETL Process Finished ---")
    return latest_timestamps

if __name__ == '__main__':
    # This block is for manual testing only.
    pass

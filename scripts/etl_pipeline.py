import os
import sys
import json
import logging
import sqlite3
import traceback
import datetime as dt
from apify_client import ApifyClient
from dotenv import load_dotenv

# --- Setup ---
# Load environment variables from a .env file in the project root
dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
load_dotenv(dotenv_path=dotenv_path)

# Add the project root to the Python path to allow imports from other directories
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

# Import the official Bittensor data models and storage class
from common.data import DataEntity, DataSource, DataLabel
from storage.miner.sqlite_miner_storage import SqliteMinerStorage

# Configure logging for the ETL process
logging.basicConfig(level=logging.INFO, format='🚚 ETL: %(asctime)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

# --- Configuration ---
APIFY_TOKEN = os.getenv("APIFY_TOKEN")
DATABASE_NAME = "miner_data.db" # This must match your miner's --neuron.database_name flag

def parse_datetime(datetime_str: str) -> dt.datetime:
    """
    Parses a datetime string from various potential formats into a timezone-aware datetime object.
    """
    # Format from Twitter Scraper: 'Wed Jul 30 23:01:51 +0000 2025'
    try:
        return dt.datetime.strptime(datetime_str, '%a %b %d %H:%M:%S %z %Y')
    except (ValueError, TypeError):
        pass
    
    # ISO format with 'Z'
    try:
        return dt.datetime.fromisoformat(str(datetime_str).replace('Z', '+00:00'))
    except (ValueError, TypeError):
        pass

    # Fallback for any other issues
    logging.warning(f"Could not parse datetime string '{datetime_str}'. Using current time as fallback.")
    return dt.datetime.now(dt.timezone.utc)


def transform_twitter_item(item: dict, label: str) -> DataEntity | None:
    """Transforms a raw Apify Twitter item into a DataEntity."""
    try:
        content_str = item.get("full_text") or item.get("text")
        url = item.get('url')
        datetime_str = item.get('created_at')

        if not content_str or not url or not datetime_str:
            logging.warning(f"Skipping Twitter item due to missing essential data. URL: {url}")
            return None
        
        content = content_str.encode('utf-8')
        
        return DataEntity(
            uri=url,
            datetime=parse_datetime(datetime_str),
            source=DataSource.X,
            label=DataLabel(value=label),
            content=content,
            content_size_bytes=len(content)
        )
    except Exception as e:
        logging.error(f"Failed to transform Twitter item: {e}. Item: {item}")
        return None

def transform_reddit_item(item: dict, label: str) -> DataEntity | None:
    """Transforms a raw Apify Reddit item into a DataEntity."""
    try:
        # The trudax/reddit-scraper-lite provides the main text in the 'body' field.
        content_text = f"{item.get('title', '')}\n\n{item.get('body', '')}".strip()
        url = item.get('url')
        # Use the correct 'createdAt' key from the JSON data
        datetime_str = item.get('createdAt')

        if not content_text or not url or not datetime_str:
            logging.warning(f"Skipping Reddit item due to missing essential data. URL: {url}")
            return None

        content = content_text.encode('utf-8')
        return DataEntity(
            uri=url,
            datetime=parse_datetime(datetime_str),
            source=DataSource.REDDIT,
            label=DataLabel(value=label),
            content=content,
            content_size_bytes=len(content)
        )
    except Exception as e:
        logging.error(f"Failed to transform Reddit item: {e}. Item: {item}")
        return None
        
def transform_youtube_item(item: dict, label: str) -> DataEntity | None:
    """Transforms a raw Apify YouTube transcript item into a DataEntity."""
    try:
        # The transcript actor returns the full transcript in a single object in a list.
        if isinstance(item, dict) and 'text' in item:
            transcript = item.get('text', '')
        # Add a fallback for other possible structures
        elif isinstance(item, dict) and 'transcript' in item:
            transcript = item.get('transcript', '')
        else:
            logging.warning(f"Unexpected YouTube transcript format. Item: {item}")
            return None

        if not transcript: 
            logging.warning(f"Skipping YouTube item due to empty transcript. URL: {item.get('url')}")
            return None

        content = transcript.encode('utf-8')
        
        return DataEntity(
            uri=item.get('url', ''),
            datetime=dt.datetime.now(dt.timezone.utc), # Transcript actor doesn't provide a timestamp
            source=DataSource.YOUTUBE,
            label=DataLabel(value=item.get('title', '')) if item.get('title') else DataLabel(value=label),
            content=content,
            content_size_bytes=len(content)
        )
    except Exception as e:
        logging.error(f"Failed to transform YouTube item: {e}. Item: {item}")
        return None

def run_etl_for_run(client: ApifyClient, storage: SqliteMinerStorage, run_id: str, transformer: callable, label: str) -> dt.datetime | None:
    """
    Extracts, transforms, and loads data from a specific Apify run.
    Returns the timestamp of the newest item processed.
    """
    latest_datetime = None
    try:
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

        # Find the newest timestamp in this batch
        latest_datetime = max(entity.datetime for entity in valid_entities)
        logging.info(f"Newest item in this batch has timestamp: {latest_datetime.isoformat()}")

        logging.info(f"Cleaning up Apify dataset for run_id: {run_id}...")
        dataset_id = client.run(run_id).get()['defaultDatasetId']
        client.dataset(dataset_id).delete()
        logging.info(f"Successfully deleted Apify dataset {dataset_id}.")
        
    except Exception:
        logging.error(f"An error occurred during ETL for run_id {run_id}: {traceback.format_exc()}")
    
    return latest_datetime

def main(run_infos: list) -> dict:
    """
    Main ETL function. Returns a dictionary of the latest timestamps for each label.
    """
    latest_timestamps = {}
    if not APIFY_TOKEN:
        logging.error("APIFY_TOKEN is not set. Exiting.")
        sys.exit(1)

    client = ApifyClient(APIFY_TOKEN)
    storage = SqliteMinerStorage(DATABASE_NAME)
    logging.info(f"--- Starting ETL Process for {len(run_infos)} successful runs ---")
    
    transformer_map = {
        DataSource.X: transform_twitter_item,
        DataSource.REDDIT: transform_reddit_item,
        DataSource.YOUTUBE: transform_youtube_item,
    }

    for run_info in run_infos:
        transformer = transformer_map.get(run_info['source'])
        if transformer:
            newest_timestamp = run_etl_for_run(
                client=client, storage=storage, run_id=run_info['run_id'],
                transformer=transformer, label=run_info['label']
            )
            if newest_timestamp:
                # Update the latest known timestamp for this specific label
                label = run_info['label']
                if label not in latest_timestamps or newest_timestamp > latest_timestamps[label]:
                    latest_timestamps[label] = newest_timestamp
        else:
            logging.warning(f"No transformer for source: {run_info['source']}.")

    logging.info("--- ETL Process Finished ---")
    return latest_timestamps

if __name__ == '__main__':
    if len(sys.argv) > 1:
        try:
            raw_run_infos = json.loads(sys.argv[1])
            for info in raw_run_infos:
                info['source'] = DataSource[info['source']]
            main(raw_run_infos)
        except (json.JSONDecodeError, KeyError) as e:
            logging.error(f"Invalid JSON provided for run_infos: {e}")
    else:
        logging.warning("No run_infos provided for manual test.")

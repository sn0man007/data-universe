# historical_etl.py
import os
import asyncio
import sqlite3
from datetime import datetime, timezone
from apify_client import ApifyClientAsync
from dotenv import load_dotenv

# Load environment variables from the .env file in the current directory
load_dotenv()

# --- Configuration ---
API_TOKEN = os.environ.get("APIFY_TOKEN")
DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'miner_data.db')
# ✅ NEW: We only need to know the ID of YOUR controller actor.
CONTROLLER_ACTOR_ID = "sn01978ms/sn13-controller-actor"


# --- ETL Functions (Copied from etl_pipeline.py for consistency) ---

async def extract_data_from_dataset(client: ApifyClientAsync, dataset_id: str) -> list:
    """Connects to Apify and fetches all items from a given dataset."""
    print(f"   EXTRACT: Fetching data from dataset: {dataset_id}")
    try:
        dataset_client = client.dataset(dataset_id)
        items_list = await dataset_client.list_items()
        print(f"   EXTRACT: Successfully extracted {len(items_list.items)} items.")
        return items_list.items
    except Exception as e:
        print(f"   EXTRACT: Error extracting data from {dataset_id}: {e}")
        return []

def transform_item_to_entity(item: dict, source: str, label: str) -> dict | None:
    """Transforms a raw scraped item into the standardized DataEntity format."""
    content = None
    url = None
    datetime_str = None
    url = item.get('url') or item.get('twitterUrl') or item.get('postUrl') or item.get('videoUrl')

    if source == 'YouTube':
        transcript_parts = item.get('transcript_full_text') or item.get('transcript') or item.get('text')
        if isinstance(transcript_parts, list):
            content = ' '.join(part.get('text', '') for part in transcript_parts)
        else:
            content = transcript_parts
    elif source == 'X':
        content = item.get("full_text") or item.get("text")
    elif source == 'Reddit':
        title = item.get('title', '')
        text_content = item.get('selftext', '')
        content = f'{title}\n\n{text_content}'.strip()

    datetime_str = item.get('datetime') or datetime.now(timezone.utc).isoformat()
    if not url or not content:
        return None
    return {'url': url, 'datetime': datetime_str, 'source': source, 'label': label, 'text': content}

def load_entities_to_db(entities: list, db_path: str):
    """Loads a list of transformed data entities into the local SQLite database."""
    valid_entities = [e for e in entities if e is not None]
    if not valid_entities:
        print("   LOAD: No valid entities to load.")
        return
    insertable_data = [(e['url'], e['datetime'], e['source'], e['label'], e['text']) for e in valid_entities]
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.executemany(
                'INSERT OR IGNORE INTO data_entities (url, datetime, source, label, text) VALUES (?, ?, ?, ?, ?)',
                insertable_data
            )
            conn.commit()
            print(f"   LOAD: Successfully inserted {cursor.rowcount} new items into the database.")
    except sqlite3.Error as e:
        print(f"   LOAD: Database error occurred: {e}")

# --- Main Historical Processing Logic ---

async def main():
    """
    Main function to fetch ALL datasets from Apify by querying the historical runs
    of the main controller actor.
    """
    if not API_TOKEN:
        raise ValueError("APIFY_TOKEN environment variable not set. Make sure it's in your .env file.")

    print("🚀 Starting Historical ETL Pipeline...")
    client = ApifyClientAsync(API_TOKEN)

    all_scraper_datasets = []
    
    print(f"🔎 Fetching all historical runs for controller actor: {CONTROLLER_ACTOR_ID}...")
    try:
        controller_runs = await client.actor(CONTROLLER_ACTOR_ID).runs().list()
        print(f"✅ Found {len(controller_runs.items)} historical controller runs to analyze.")
        
        for i, run_summary in enumerate(controller_runs.items):
            controller_run_id = run_summary.get("id")
            controller_dataset_id = run_summary.get("defaultDatasetId")
            print(f"\n   Analyzing controller run {i+1}/{len(controller_runs.items)} (ID: {controller_run_id})...")

            if not controller_dataset_id:
                print("   ⚠️ This controller run has no output dataset. Skipping.")
                continue
            
            # The output of the controller contains the list of scraper jobs it started.
            scraper_jobs = await client.dataset(controller_dataset_id).list_items()
            print(f"   Found {len(scraper_jobs.items)} scraper jobs in this controller run's output.")
            
            for job in scraper_jobs.items:
                if job.get("datasetId"):
                    all_scraper_datasets.append(job)

    except Exception as e:
        print(f"   🔥 Could not fetch runs for controller actor {CONTROLLER_ACTOR_ID}: {e}")

    print(f"\n✅ Found a total of {len(all_scraper_datasets)} historical scraper datasets to process.")
    
    if not all_scraper_datasets:
        print("⏹️ No datasets found from any controller runs. Exiting.")
        return

    for i, dataset_info in enumerate(all_scraper_datasets):
        dataset_id = dataset_info.get("datasetId")
        source = dataset_info.get("source", "Unknown")
        label = dataset_info.get("label", "historical_import")

        print(f"\n({i+1}/{len(all_scraper_datasets)}) Processing dataset for '{source}/{label}' (ID: {dataset_id})...")

        if source == "Unknown":
            print("   ⚠️ Cannot determine source for this dataset. Skipping.")
            continue

        raw_items = await extract_data_from_dataset(client, dataset_id)

        if not raw_items:
            print("   No items to process in this dataset. Skipping.")
            continue

        transformed_entities = [transform_item_to_entity(item, source, label) for item in raw_items]
        load_entities_to_db(transformed_entities, DB_PATH)

    print("\n✅ Historical ETL Pipeline finished successfully.")

if __name__ == "__main__":
    asyncio.run(main())

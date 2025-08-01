import os
import sys
import json
import random
import logging
import datetime as dt
from apify_client import ApifyClient
from dotenv import load_dotenv

# --- Setup ---
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

dotenv_path = os.path.join(project_root, '.env')
load_dotenv(dotenv_path=dotenv_path)

from neurons.target_provider import get_scraping_targets
from scripts.etl_pipeline import main as run_etl
from common.data import DataSource

logging.basicConfig(level=logging.INFO, format='🤖 ORCHESTRATOR: %(asctime)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

# --- Configuration ---
APIFY_TOKEN = os.getenv("APIFY_TOKEN")
STATE_FILE_PATH = os.path.join(project_root, "scripts", "pipeline_state.json")

X_SCRAPER_ACTOR_ID = "xtdata/twitter-x-scraper"
REDDIT_SCRAPER_ACTOR_ID = "trudax/reddit-scraper-lite"
YOUTUBE_DISCOVERY_ACTOR_ID = "streamers/youtube-scraper"
YOUTUBE_TRANSCRIPT_ACTOR_ID = "smartly_automated/youtube-transcript-scraper-premium-version"

# --- State Management (Watermarking) ---

def load_pipeline_state() -> dict:
    """Loads the last run timestamps from the state file."""
    if not os.path.exists(STATE_FILE_PATH):
        return {}
    try:
        with open(STATE_FILE_PATH, 'r') as f:
            state = json.load(f)
            # Convert timestamp strings back to datetime objects
            return {label: dt.datetime.fromisoformat(ts) for label, ts in state.items()}
    except (json.JSONDecodeError, IOError):
        logging.warning("Could not read or parse state file. Starting from scratch.")
        return {}

def save_pipeline_state(state: dict):
    """Saves the latest timestamps to the state file."""
    try:
        with open(STATE_FILE_PATH, 'w') as f:
            # Convert datetime objects to ISO format strings for JSON serialization
            serializable_state = {label: ts.isoformat() for label, ts in state.items()}
            json.dump(serializable_state, f, indent=4)
        logging.info(f"Successfully saved pipeline state to {STATE_FILE_PATH}")
    except IOError:
        logging.error(f"Could not write to state file at {STATE_FILE_PATH}")

# --- Core Orchestration Logic ---

def trigger_and_wait(client: ApifyClient, actor_id: str, run_input: dict, label: str, source: DataSource) -> dict | None:
    """Triggers an Apify actor, waits for it to finish, and returns run info on success."""
    try:
        logging.info(f"Triggering actor '{actor_id}' for label '{label}' with input: {run_input}")
        actor_run = client.actor(actor_id).call(run_input=run_input)
        run_id = actor_run['id']
        logging.info(f"Actor '{actor_id}' started! Run ID: {run_id}")
        
        logging.info(f"Waiting for run {run_id} to complete...")
        run_details = client.run(run_id).wait_for_finish()

        if run_details['status'] == 'SUCCEEDED':
            logging.info(f"✅ Apify run {run_id} for '{label}' completed successfully.")
            return {'run_id': run_id, 'source': source, 'label': label}
        else:
            logging.error(f"❌ Apify run {run_id} for '{label}' did not succeed. Status: {run_details['status']}.")
            return None
    except Exception:
        logging.error(f"An error occurred while running actor {actor_id} for '{label}': {traceback.format_exc()}")
        return None

def main_cycle():
    """ Main function to run one full cycle of the data pipeline. """
    if not APIFY_TOKEN:
        logging.error("APIFY_TOKEN is not set. Exiting.")
        sys.exit(1)

    logging.info("--- Starting New Data Pipeline Cycle ---")
    client = ApifyClient(APIFY_TOKEN)
    pipeline_state = load_pipeline_state()
    
    logging.info("Fetching dynamic targets...")
    try:
        targets = get_scraping_targets()
        scraper_configs = targets.get('scraper_configs', {})
    except Exception as e:
        logging.error(f"Could not get dynamic targets: {e}. Aborting cycle.")
        return

    successful_run_infos = []

    # Trigger X Scraper
    if 'X.flash' in scraper_configs:
        for label_config in scraper_configs['X.flash'].labels_to_scrape:
            if label_config.label_choices:
                label = random.choice(label_config.label_choices)
                # --- DEFINITIVE FIX: Increase maxItems to satisfy actor requirements ---
                run_input = {"searchTerms": [label], "maxItems": 50}
                # --- END FIX ---
                if label in pipeline_state:
                    run_input["since"] = pipeline_state[label].strftime('%Y-%m-%d')
                
                run_info = trigger_and_wait(client, X_SCRAPER_ACTOR_ID, run_input, label, DataSource.X)
                if run_info: successful_run_infos.append(run_info)

    # Trigger Reddit Scraper
    if 'Reddit.lite' in scraper_configs:
        for label_config in scraper_configs['Reddit.lite'].labels_to_scrape:
            if label_config.label_choices:
                label = random.choice(label_config.label_choices)
                run_input = {"searches": [label], "maxItems": 50, "sortBy": "new"}
                run_info = trigger_and_wait(client, REDDIT_SCRAPER_ACTOR_ID, run_input, label, DataSource.REDDIT)
                if run_info: successful_run_infos.append(run_info)

    # Trigger YouTube Discovery (Stage 1)
    youtube_video_urls = []
    if 'YouTube.custom.transcript' in scraper_configs:
        for label_config in scraper_configs['YouTube.custom.transcript'].labels_to_scrape:
            if label_config.label_choices:
                label = random.choice(label_config.label_choices)
                run_input = {"searchQueries": [label], "maxVideos": 5}
                if label in pipeline_state:
                    run_input["uploadedAfter"] = "7_days_ago"
                
                run_info = trigger_and_wait(client, YOUTUBE_DISCOVERY_ACTOR_ID, run_input, label, DataSource.YOUTUBE)
                if run_info:
                    dataset_items = client.run(run_info['run_id']).dataset().list_items().items
                    for item in dataset_items:
                        url = item.get('url') or item.get('videoUrl')
                        if url: youtube_video_urls.append(url)
                    client.dataset(client.run(run_info['run_id']).get()['defaultDatasetId']).delete()

    # Trigger YouTube Transcripts (Stage 2)
    if youtube_video_urls:
        # --- DEFINITIVE FIX: Format URLs as a list of objects ---
        run_input = {"video_urls": [{"url": url} for url in youtube_video_urls]}
        # --- END FIX ---
        run_info = trigger_and_wait(client, YOUTUBE_TRANSCRIPT_ACTOR_ID, run_input, "transcript_batch", DataSource.YOUTUBE)
        if run_info:
            run_info['source'] = 'YOUTUBE_TRANSCRIPTS' 
            successful_run_infos.append(run_info)

    # ETL PHASE & UPDATE STATE
    if successful_run_infos:
        logging.info("Starting the ETL process...")
        etl_ready_infos = []
        for info in successful_run_infos:
            source = info['source']
            if source == 'YOUTUBE_TRANSCRIPTS': source = DataSource.YOUTUBE
            etl_ready_infos.append({'run_id': info['run_id'], 'source': source, 'label': info['label']})
        
        new_timestamps = run_etl(etl_ready_infos)
        
        pipeline_state.update(new_timestamps)
        save_pipeline_state(pipeline_state)
        
        logging.info("ETL process finished.")
    else:
        logging.error("All scrapers failed. Skipping ETL.")

    logging.info("--- Data Pipeline Cycle Finished ---")

if __name__ == "__main__":
    main_cycle()

import os
import sys
import json
import random
import logging
import traceback
import datetime as dt
from apify_client import ApifyClient
from dotenv import load_dotenv

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)
dotenv_path = os.path.join(project_root, '.env')
load_dotenv(dotenv_path=dotenv_path)

from neurons.target_provider import get_scraping_targets
from scripts.etl_pipeline import main as run_etl
from common.data import DataSource
from storage.miner.sqlite_miner_storage import PostgresMinerStorage

logger = logging.getLogger("ORCHESTRATOR")
logging.basicConfig(level=logging.INFO, format='🤖 %(name)s: %(asctime)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

APIFY_TOKEN = os.getenv("APIFY_TOKEN")
STATE_FILE_PATH = os.path.join(project_root, "pipeline_state.json")
X_SCRAPER_ACTOR_ID = "xtdata/twitter-x-scraper"
REDDIT_SCRAPER_ACTOR_ID = "trudax/reddit-scraper-lite"
YOUTUBE_DISCOVERY_ACTOR_ID = "streamers/youtube-scraper"
YOUTUBE_TRANSCRIPT_ACTOR_ID = "smartly_automated/youtube-transcript-scraper-premium-version"

MONTHLY_BUDGET_USD = 135.0
RUNS_PER_DAY = 24
COST_PER_1000_X = 0.50
COST_PER_1000_REDDIT = 2.00
COST_PER_1000_YOUTUBE_DISCOVERY = 5.00
COST_PER_1000_YOUTUBE_TRANSCRIPT = 20.00

def calculate_per_run_limits() -> dict:
    daily_budget = MONTHLY_BUDGET_USD / 30.0
    per_run_budget = daily_budget / RUNS_PER_DAY
    budget_x = per_run_budget * 0.4
    budget_reddit = per_run_budget * 0.4
    budget_youtube = per_run_budget * 0.2
    limit_x = int((budget_x / COST_PER_1000_X) * 1000)
    limit_reddit = int((budget_reddit / COST_PER_1000_REDDIT) * 1000)
    limit_youtube_videos = max(1, int((budget_youtube / (COST_PER_1000_YOUTUBE_DISCOVERY + COST_PER_1000_YOUTUBE_TRANSCRIPT)) * 1000))
    limits = { "x_per_run_limit": limit_x, "reddit_per_run_limit": limit_reddit, "youtube_per_run_limit": limit_youtube_videos }
    logger.info(f"Calculated per-run scraping limits: {limits}")
    return limits

def load_pipeline_state() -> dict:
    if not os.path.exists(STATE_FILE_PATH): return {}
    try:
        with open(STATE_FILE_PATH, 'r') as f:
            state = json.load(f)
            return {label: dt.datetime.fromisoformat(ts) for label, ts in state.items()}
    except (json.JSONDecodeError, IOError):
        return {}

def save_pipeline_state(state: dict):
    with open(STATE_FILE_PATH, 'w') as f:
        json.dump({label: ts.isoformat() for label, ts in state.items()}, f, indent=4)
    logger.info(f"Successfully saved pipeline state.")

def trigger_and_wait(client: ApifyClient, actor_id: str, run_input: dict, label: str, source: DataSource) -> dict | None:
    try:
        logger.info(f"Triggering actor '{actor_id}' for label '{label}'")
        run = client.actor(actor_id).call(run_input=run_input)
        run_details = client.run(run['id']).wait_for_finish()
        if run_details['status'] == 'SUCCEEDED':
            logger.info(f"✅ Apify run {run['id']} completed successfully.")
            return {'run_id': run['id'], 'source': source, 'label': label}
        else:
            return None
    except Exception:
        return None

def main_cycle():
    logger.info("--- Starting New Data Pipeline Cycle ---")
    try:
        storage = PostgresMinerStorage()
        logger.info("✅ Pre-flight check complete. Database is ready.")
    except Exception:
        logger.error(f"❌ Pre-flight check failed. Could not initialize database: {traceback.format_exc()}")
        return

    if not APIFY_TOKEN:
        logger.error("APIFY_TOKEN is not set. Exiting.")
        return

    client = ApifyClient(APIFY_TOKEN)
    pipeline_state = load_pipeline_state()
    per_run_limits = calculate_per_run_limits()

    targets = get_scraping_targets()
    scraper_configs = targets.get('scraper_configs', {})
    successful_run_infos = []

    if 'X.flash' in scraper_configs:
        for config in scraper_configs['X.flash'].labels_to_scrape:
            if config.label_choices:
                label = random.choice(config.label_choices)
                run_input = {"searchTerms": [label], "maxItems": per_run_limits['x_per_run_limit'], "sort": "Latest"}
                if label in pipeline_state: run_input["since"] = pipeline_state[label].strftime('%Y-%m-%d')
                run_info = trigger_and_wait(client, X_SCRAPER_ACTOR_ID, run_input, label, DataSource.X)
                if run_info: successful_run_infos.append(run_info)

    if 'Reddit.lite' in scraper_configs:
        for config in scraper_configs['Reddit.lite'].labels_to_scrape:
            if config.label_choices:
                label = random.choice(config.label_choices)
                run_input = {"searches": [label], "maxItems": per_run_limits['reddit_per_run_limit'], "sortBy": "new"}
                run_info = trigger_and_wait(client, REDDIT_SCRAPER_ACTOR_ID, run_input, label, DataSource.REDDIT)
                if run_info: successful_run_infos.append(run_info)

    youtube_video_urls = []
    if 'YouTube.custom.transcript' in scraper_configs:
        for config in scraper_configs['YouTube.custom.transcript'].labels_to_scrape:
            if config.label_choices:
                label = random.choice(config.label_choices)
                run_input = {"searchQueries": [label], "maxVideos": per_run_limits['youtube_per_run_limit']}
                run_info = trigger_and_wait(client, YOUTUBE_DISCOVERY_ACTOR_ID, run_input, label, DataSource.YOUTUBE)
                if run_info:
                    dataset = client.run(run_info['run_id']).dataset().list_items().items
                    for item in dataset:
                        if url := item.get('url') or item.get('videoUrl'):
                            youtube_video_urls.append({'url': url, 'title': item.get('title', label)})
                    client.run(run_info['run_id']).delete()

    if youtube_video_urls:
        run_input = {"video_urls": youtube_video_urls}
        run_info = trigger_and_wait(client, YOUTUBE_TRANSCRIPT_ACTOR_ID, run_input, "transcript_batch", DataSource.YOUTUBE)
        if run_info: successful_run_infos.append(run_info)

    if successful_run_infos:
        etl_infos = [{'run_id': info['run_id'], 'source': info['source'], 'label': info['label']} for info in successful_run_infos]
        new_timestamps = run_etl(etl_infos, storage)
        pipeline_state.update(new_timestamps)
        save_pipeline_state(pipeline_state)
    else:
        logger.warning("No successful scraper runs. Skipping ETL.")
    logger.info("--- Data Pipeline Cycle Finished ---")

if __name__ == "__main__":
    main_cycle()

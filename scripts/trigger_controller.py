# trigger_controller.py

import asyncio
import os
import sys
from apify_client import ApifyClientAsync
from dotenv import load_dotenv
import importlib.util

# ✅ FINAL FIX: Use importlib for a robust, explicit import.
# This bypasses potential sys.path issues.
try:
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    module_path = os.path.join(project_root, "desirability_manager.py")
    spec = importlib.util.spec_from_file_location("desirability_manager", module_path)
    desirability_manager_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(desirability_manager_module)
    DesirabilityManager = desirability_manager_module.DesirabilityManager
except FileNotFoundError:
    print("FATAL: desirability_manager.py not found in the parent directory.", file=sys.stderr)
    sys.exit(1)


# Load environment variables from .env file
# We need to specify the path to the .env file in the scripts directory
dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path=dotenv_path)

# --- Configuration ---
API_TOKEN = os.environ.get("APIFY_TOKEN")
CONTROLLER_ACTOR_ID = "sn01978ms/sn13-controller-actor"

# Define the specific Actor IDs for the scrapers.
# These are the official IDs for the public actors.
SCRAPER_ACTOR_IDS = {
    "x_scraper_id": "xtdata/twitter-x-scraper",
    "reddit_scraper_id": "autoscraping/reddit-keyword-post-scraper",
    "youtube_scraper_id": "streamers/youtube-scraper"
}

async def main():
    """
    Main function to trigger the controller actor with dynamic targets.
    """
    print("▶️ Starting the trigger script...")
    if not API_TOKEN:
        raise ValueError("APIFY_TOKEN environment variable not set. Make sure it's in your .env file.")

    print("✅ Apify Client Initialized.")
    
    # Initialize the desirability manager to get targets
    desirability_manager = DesirabilityManager()
    
    print("🎯 Fetching dynamic targets for this run from the SN13 Dashboard...")
    targets = await desirability_manager.get_desirable_targets()
    print(f"✅ Targets for this run: {targets}")

    # Combine the scraping targets with the scraper actor IDs
    run_input = {
        **targets,
        **SCRAPER_ACTOR_IDS
    }

    print(f"🚀 Triggering the '{CONTROLLER_ACTOR_ID}' with direct input...")
    
    try:
        apify_client = ApifyClientAsync(API_TOKEN)
        controller_actor = apify_client.actor(CONTROLLER_ACTOR_ID)
        
        # Start the actor and wait for it to finish
        run = await controller_actor.call(run_input=run_input)
        
        print(f"✅ Controller Actor started successfully! Run ID: {run['id']}")
        print(f"  Monitor the run here: https://console.apify.com/actors/runs/{run['id']}")

    except Exception as e:
        print(f"🔥 An error occurred while triggering the controller actor: {e}")

    print("\n⏹️ Trigger script finished.")

if __name__ == "__main__":
    asyncio.run(main())

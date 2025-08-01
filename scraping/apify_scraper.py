import asyncio
from apify_client import ApifyClient
import configparser

class ApifyScraper:
    """
    A unified client for running scrapers ("Actors") on the Apify platform.
    """
    def __init__(self, api_token: str):
        self.client = ApifyClient(api_token)
        print("ApifyScraper initialized.")

    async def run_actor(self, actor_id: str, actor_input: dict) -> list:
        """
        Runs a specific Apify Actor with the given input and waits for the results.
        """
        try:
            print(f"Starting Apify Actor: {actor_id}...")
            run = await asyncio.to_thread(self.client.actor(actor_id).call, run_input=actor_input)
            
            print(f"Actor {actor_id} finished. Fetching results...")
            items = []
            dataset = await asyncio.to_thread(self.client.dataset(run["defaultDatasetId"]).list_items)
            
            for item in dataset.items:
                items.append(item)
            
            print(f"Successfully fetched {len(items)} items from Actor {actor_id}.")
            return items

        except Exception as e:
            print(f"An error occurred while running Apify Actor {actor_id}: {e}")
            return []

# --- Example Usage for Testing ---
async def main():
    config = configparser.ConfigParser()
    config.read('scraping/config.ini')
    apify_token = config['Apify'].get('api_token')

    if "YOUR_APIFY_API_TOKEN" in apify_token:
        print("Please add your Apify API token to scraping/config.ini")
        return

    scraper = ApifyScraper(api_token=apify_token)

    # Use the powerful "apidojo/tweet-scraper-v2" Actor
    twitter_actor_id = "apidojo/tweet-scraper-v2"
    
    # Configure the input for the Actor to scrape the bittensor_ profile
    twitter_input = {
        "urls": ["https://x.com/bittensor_"],
        "maxItems": 5,
        "addUserInfo": True
    }
    
    tweets = await scraper.run_actor(twitter_actor_id, twitter_input)

    print("\n--- Scraped Tweets (via Apify) ---")
    if tweets:
        for tweet in tweets:
            # The structure of 'tweet' depends on the Actor's output format.
            # We print the user and text fields for clarity.
            user_info = tweet.get('user', {})
            print(f"User: @{user_info.get('userName')}\nText: {tweet.get('text')}\n---")

if __name__ == "__main__":
    asyncio.run(main())

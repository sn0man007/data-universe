import asyncpraw
import asyncio
import configparser

class RedditScraper:
    """
    A scraper for fetching data from Reddit using the asyncpraw library.
    This is the official and most reliable method.
    """

    def __init__(self, client_id: str, client_secret: str, user_agent: str):
        """
        Initializes the scraper with Reddit API credentials.
        """
        self.reddit = asyncpraw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            user_agent=user_agent,
        )
        print("RedditScraper initialized and authenticated.")

    async def get_posts(self, subreddit_name: str, limit: int = 25):
        """
        Fetches the latest posts from a given subreddit.
        """
        try:
            print(f"Fetching {limit} posts from r/{subreddit_name}...")
            subreddit = await self.reddit.subreddit(subreddit_name)
            posts_data = []
            async for submission in subreddit.hot(limit=limit):
                posts_data.append({
                    'id': submission.id,
                    'title': submission.title,
                    'score': submission.score,
                    'url': submission.url,
                    'selftext': submission.selftext,
                    'created_utc': submission.created_utc
                })
            print(f"Successfully fetched {len(posts_data)} posts.")
            return posts_data
        except Exception as e:
            print(f"An error occurred while fetching from Reddit: {e}")
            return []

    async def __aenter__(self):
        """Allows the class to be used as an async context manager."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Closes the Reddit session when exiting the context."""
        await self.reddit.close()
        print("Reddit session closed.")


# --- Example Usage ---
async def main():
    # Read credentials from the config file
    config = configparser.ConfigParser()
    config.read('scraping/config.ini')

    reddit_config = config['Reddit']
    client_id = reddit_config.get('client_id')
    client_secret = reddit_config.get('client_secret')
    user_agent = reddit_config.get('user_agent')

    if not client_id or "YOUR_NEW_CLIENT_ID" in client_id:
        print("Error: Please add your new Reddit credentials to scraping/config.ini")
        return

    # Using 'async with' ensures the network session is properly closed.
    async with RedditScraper(
        client_id=client_id,
        client_secret=client_secret,
        user_agent=user_agent
    ) as scraper:
        # Fetch posts from the correct r/bittensor_ subreddit (with an underscore)
        posts = await scraper.get_posts("bittensor_", limit=5)

        print("\n--- Fetched Reddit Posts ---")
        for post in posts:
            print(f"Title: {post['title']}\nURL: {post['url']}\n---")

if __name__ == "__main__":
    asyncio.run(main())

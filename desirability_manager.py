# /home/sn0man/projects/data-universe/desirability_manager.py

import httpx
import logging
import json

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='🧠 DESIRABILITY_MANAGER: %(message)s')

class DesirabilityManager:
    """
    Manages fetching desirable targets for the scrapers, with a fallback mechanism.
    """
    def __init__(self, api_url="https://sn13-dashboard.api.macrocosmos.ai/api/desirability"):
        """
        Initializes the DesirabilityManager.
        Args:
            api_url (str): The URL of the dashboard API to fetch dynamic targets from.
        """
        self.api_url = api_url
        self.client = httpx.AsyncClient(timeout=10.0)

    async def get_dynamic_targets(self) -> dict | None:
        """
        Asynchronously fetches the dynamic desirability list from the dashboard API.
        Returns:
            A dictionary of targets if successful, otherwise None.
        """
        logging.info("Querying the SN13 Dashboard API for dynamic desirability...")
        try:
            response = await self.client.get(self.api_url)
            response.raise_for_status()  # Raises an exception for 4xx or 5xx status codes
            return response.json()
        except json.JSONDecodeError:
            logging.warning("Failed to decode JSON from API response. The API might be down or returning non-JSON content.")
            # Log the first 500 characters of the response to help diagnose the issue.
            logging.warning(f"Response text (first 500 chars): {response.text[:500]}")
            return None
        except httpx.HTTPStatusError as e:
            logging.warning(f"Failed to fetch data from the Dashboard API. Status code: {e.response.status_code}")
            return None
        except Exception as e:
            logging.warning(f"Failed to fetch data from the Dashboard API. Error: {e}")
            return None

    def get_static_fallback_targets(self) -> dict:
        """
        Provides an updated, hardcoded list of high-value targets to be used
        if the dynamic fetch fails. This ensures the miner can always continue to operate.
        This list is based on a strategic analysis of communities relevant to Bittensor.
        """
        logging.info("Using updated, high-value static fallback targets.")
        return {
            'x_handles': [
                '#bittensor', '#tao', '#crypto', '#btc', '#bitcoin',
                '#cryptocurrency', '#ai', '#decentralized', '#web3',
                '#machinelearning', '#llm', '#defi', '#blockchain '
            ],
            # ======================================================================
            # CORRECTED LINE:
            # The original error was on the line below. A key ('reddit_subreddits')
            # must be followed by a colon and a value. Here, the value is a list
            # of strings, where each string is the name of a subreddit.
            # ======================================================================
            'reddit_subreddits': [
                'bittensor_',
                'CryptoCurrency',
                'datascience',
                'MachineLearning',
                'artificial',
                'singularity',
                'decentralizedAI',
            ],
            'youtube_keywords': [
                'bittensor', 'decentralized ai', 'opentensor', 'artificial intelligence',
                'machine learning tutorial', 'crypto analysis', 'latest tech news'
            ]
        }

    async def get_desirable_targets(self) -> dict:
        """
        The main method to get targets. It first tries to fetch dynamic targets
        and falls back to a static list if the dynamic fetch fails.
        """
        dynamic_targets = await self.get_dynamic_targets()
        if dynamic_targets:
            logging.info("Successfully fetched dynamic targets from the dashboard.")
            return dynamic_targets
        else:
            logging.warning("Falling back to static targets.")
            return self.get_static_fallback_targets()

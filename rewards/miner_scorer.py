import threading
from typing import List
import torch
import bittensor as bt

from common.data import ScorableMinerIndex
from rewards.reward_distribution import RewardDistribution
from scraping.scraper import ValidationResult


class MinerScorer:
    """Tracks the score of each miner and handles updates to the scores.

    Thread safe.
    """

    # Start new miner's at a 50% credibility.
    STARTING_CREDIBILITY = 0.5

    def __init__(
        self,
        num_neurons: int,
        reward_distribution: RewardDistribution,
        alpha: float = 0.05,
    ):
        # Tracks the raw scores of each miner. i.e. not the weights that are set on the blockchain.
        self.scores = torch.zeros(num_neurons, dtype=torch.float32)
        self.miner_credibility = torch.full(
            (num_neurons, 1), MinerScorer.STARTING_CREDIBILITY, dtype=torch.float32
        )
        self.reward_distribution = reward_distribution
        self.alpha = alpha

        # Make this class thread safe because it'll eventually be accessed by multiple threads.
        # One from the main validator evaluation loop and another from a background thread performing validation on user requests.
        self.lock = threading.Lock()

    def get_scores(self) -> torch.Tensor:
        """Returns the raw scores of all miners."""
        # Return a copy to ensure outside code can't modify the scores.
        with self.lock:
            return self.scores.clone()

    def reset_score(self, uid: int) -> None:
        """Resets the score of the 'uid' miner to 0."""
        with self.lock:
            self.scores[uid] = 0.0

    def resize(self, num_neurons: int) -> None:
        """Resizes the score tensor to the new number of neurons.

        The new size must be greater than or equal to the current size.
        """
        with self.lock:
            assert num_neurons >= self.scores.size(
                0
            ), f"Tried to downsize the number of neurons from {self.scores.size(0)} to {num_neurons}"
            to_add = num_neurons - self.scores.size(0)
            self.scores = torch.cat(
                [self.scores, torch.zeros(to_add, dtype=torch.float32)]
            )
            self.miner_credibility = torch.cat(
                [
                    self.miner_credibility,
                    torch.full(
                        (to_add, 1),
                        MinerScorer.STARTING_CREDIBILITY,
                        dtype=torch.float32,
                    ),
                ]
            )

    def on_miner_evaluated(
        self,
        uid: int,
        index: ScorableMinerIndex,
        validation_results: List[ValidationResult],
    ) -> None:
        """Notifies the scorer that a miner has been evaluated and should have its score updated.

        Args:
            uid (int): The miner's UID.
            index (MinerIndex): The latest index of the miner.
            validation_results (List[ValidationResult]): The results of data validation performed on the data provided by the miner.
        """
        with self.lock:
            # First, update the miner's credibilty
            self._update_credibility(uid, validation_results)

            # Now score the miner based on the amount of data it has, scaled based on
            # the reward distribution.
            score = 0.0
            for chunk in index.scorable_chunks:
                score += self.reward_distribution.get_score_for_chunk(chunk)

            # Scale the miner's score by its credibility, squared.
            score *= self.miner_credibility[uid] ** 2

            self._update_score(uid, score)

            bt.logging.trace(
                f"Evaluated Miner {uid}. Score={self.scores[uid]}. Credibility={self.miner_credibility[uid]}"
            )

    def _update_credibility(self, uid: int, validation_results: List[ValidationResult]):
        """Updates the miner's credibility based on the most recent set of validation_results.

        Requires: self.lock is held.
        """
        assert (
            len(validation_results) > 0
        ), "Must be provided at least 1 validation result."

        # Use EMA to update the miner's credibility.
        credibility = sum(result.is_valid for result in validation_results) / float(
            len(validation_results)
        )
        self.miner_credibility[uid] = (
            self.alpha * credibility + (1 - self.alpha) * self.miner_credibility[uid]
        )

    def _update_score(self, uid: int, reward: float):
        """Performs exponential moving average on the scores based on the rewards received from the miners.

        Requires: self.lock is held.
        """

        bt.logging.trace(
            f"Updating miner {uid}'s score with reward {reward}. Current score = {self.scores[uid]}"
        )
        self.scores[uid] = self.alpha * reward + (1 - self.alpha) * self.scores[uid]
        bt.logging.trace(f"Updated miner {uid}'s score to {self.scores[uid]}")

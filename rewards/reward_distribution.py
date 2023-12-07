import datetime as dt
from common.data import DataLabel, DataSource, TimeBucket, ScorableDataChunkSummary
from rewards.data import RewardDistributionModel

import rewards.distributions.disitribution_v1 as v1


class RewardDistribution:
    """Describes how rewards are distributed across DataSources and DataLabels."""

    def __init__(self, model: RewardDistributionModel = v1.DISTRIBUTION):
        self.model = model

    def get_score_for_chunk(self, chunk: ScorableDataChunkSummary) -> float:
        """Returns the score for the given chunk.

        A chunk is scored as follows:
            1. It is weighted based on the weight of its data source.
            2. It's scaled based on the Label. This may be negative if the data is undesirable.
            3. It's scaled based on the age of the data, where newer data is considered more valuable.
        """

        data_type_scale_factor = self._scale_factor_for_source_and_label(
            chunk.source, chunk.label
        )
        time_scalar = self._scale_factor_for_age(chunk.time_bucket)
        return data_type_scale_factor * time_scalar * chunk.scorable_bytes

    def _scale_factor_for_source_and_label(
        self, data_source: DataSource, label: DataLabel
    ) -> float:
        """Returns the score scalar for the given data source and label."""
        data_source_reward = self.model.distribution[data_source]
        label_factor = data_source_reward.label_scale_factors.get(
            label, data_source_reward.default_scale_factor
        )
        return data_source_reward.weight * label_factor

    def _scale_factor_for_age(self, time_bucket: TimeBucket) -> float:
        """Returns the score scalar for data ."""
        # Data age is scored using a linear depreciation function, where data from now is scored 1 and data
        # that is max_age_in_hours old is scored 0.5.
        # All data older than max_age_in_hours is scored 0.
        data_age_in_hours = (
            dt.datetime.now(tz=dt.timezone.utc) - time_bucket.get_date_range().start
        ).total_seconds() // 3600

        # Safe guard against future data.
        data_age_in_hours = max(0, data_age_in_hours)

        if data_age_in_hours > self.model.max_age_in_hours:
            return 0.0
        return 1.0 - (data_age_in_hours / (2 * self.model.max_age_in_hours))

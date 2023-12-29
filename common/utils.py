"""General utility functions."""

import datetime as dt
import pickle
import time
from math import floor
from typing import Any, Callable
import bittensor as bt
from functools import lru_cache, update_wrapper

from common.date_range import DateRange

_KB = 1024
_MB = 1024 * _KB
_GB = 1024 * _MB


def mb_to_bytes(mb: int) -> int:
    """Returns the total number of bytes."""
    return mb * _MB


def gb_to_bytes(gb: int) -> int:
    """Returns the total number of bytes."""
    return gb * _GB


def seconds_to_hours(seconds: int) -> int:
    """Returns the total number of hours, rounded down."""
    return seconds // 3600


def datetime_from_hours_since_epoch(hours: int) -> dt.datetime:
    """Returns a datetime object from the provided hours since epoch."""
    return dt.datetime.fromtimestamp(hours * 3600, tz=dt.timezone.utc)


def is_miner(uid: int, metagraph: bt.metagraph) -> bool:
    """Checks if a UID on the subnet is a miner."""
    # Assume everyone who isn't a validator is a miner.
    # This explicilty disallows validator/miner hybrids.
    return metagraph.Tv[uid] == 0


def is_validator(uid: int, metagraph: bt.metagraph) -> bool:
    """Checks if a UID on the subnet is a validator."""
    return metagraph.validator_permit[uid] and metagraph.S[uid] >= 512


def time_bucket_id_from_datetime(datetime: dt.datetime) -> int:
    """Returns the Timebucket ID from the provided datetime.

    Args:
        datetime (datetime.datetime): A datetime object, assumed to be in UTC.
    """
    return seconds_to_hours(datetime.astimezone(tz=dt.timezone.utc).timestamp())


@classmethod
def time_bucket_id_to_date_range(bucket: int) -> DateRange:
    """Returns the date range from a Timebucket ID."""
    return DateRange(
        start=datetime_from_hours_since_epoch(bucket),
        end=datetime_from_hours_since_epoch(bucket + 1),
    )


def serialize_to_file(obj: Any, filename: str) -> None:
    """
    Serializes 'obj' and writes it to 'filename'
    """
    with open(filename, "wb") as file:
        pickle.dump(obj, file)


def deserialize_from_file(filename: str) -> Any:
    """
    Deserialize an object from a file.
    """
    with open(filename, "rb") as file:
        obj = pickle.load(file)
    return obj


# LRU Cache with TTL
def ttl_cache(maxsize: int = 128, typed: bool = False, ttl: int = -1):
    """
    Decorator that creates a cache of the most recently used function calls with a time-to-live (TTL) feature.
    The cache evicts the least recently used entries if the cache exceeds the `maxsize` or if an entry has
    been in the cache longer than the `ttl` period.

    Args:
        maxsize (int): Maximum size of the cache. Once the cache grows to this size, subsequent entries
                       replace the least recently used ones. Defaults to 128.
        typed (bool): If set to True, arguments of different types will be cached separately. For example,
                      f(3) and f(3.0) will be treated as distinct calls with distinct results. Defaults to False.
        ttl (int): The time-to-live for each cache entry, measured in seconds. If set to a non-positive value,
                   the TTL is set to a very large number, effectively making the cache entries permanent. Defaults to -1.

    Returns:
        Callable: A decorator that can be applied to functions to cache their return values.

    The decorator is useful for caching results of functions that are expensive to compute and are called
    with the same arguments frequently within short periods of time. The TTL feature helps in ensuring
    that the cached values are not stale.

    Example:
        @ttl_cache(ttl=10)
        def get_data(param):
            # Expensive data retrieval operation
            return data
    """
    if ttl <= 0:
        ttl = 65536
    hash_gen = _ttl_hash_gen(ttl)

    def wrapper(func: Callable) -> Callable:
        @lru_cache(maxsize, typed)
        def ttl_func(ttl_hash, *args, **kwargs):
            return func(*args, **kwargs)

        def wrapped(*args, **kwargs) -> Any:
            th = next(hash_gen)
            return ttl_func(th, *args, **kwargs)

        return update_wrapper(wrapped, func)

    return wrapper


def _ttl_hash_gen(seconds: int):
    """
    Internal generator function used by the `ttl_cache` decorator to generate a new hash value at regular
    time intervals specified by `seconds`.

    Args:
        seconds (int): The number of seconds after which a new hash value will be generated.

    Yields:
        int: A hash value that represents the current time interval.

    This generator is used to create time-based hash values that enable the `ttl_cache` to determine
    whether cached entries are still valid or if they have expired and should be recalculated.
    """
    start_time = time.time()
    while True:
        yield floor((time.time() - start_time) / seconds)


# 12 seconds updating block.
@ttl_cache(maxsize=1, ttl=12)
def ttl_get_block(self) -> int:
    """
    Retrieves the current block number from the blockchain. This method is cached with a time-to-live (TTL)
    of 12 seconds, meaning that it will only refresh the block number from the blockchain at most every 12 seconds,
    reducing the number of calls to the underlying blockchain interface.

    Returns:
        int: The current block number on the blockchain.

    This method is useful for applications that need to access the current block number frequently and can
    tolerate a delay of up to 12 seconds for the latest information. By using a cache with TTL, the method
    efficiently reduces the workload on the blockchain interface.

    Example:
        current_block = ttl_get_block(self)

    Note: self here is the miner or validator instance
    """
    return self.subtensor.get_current_block()

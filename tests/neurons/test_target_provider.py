import pytest
from unittest.mock import patch
import os
import json

# We need to tell Python where to find the project's root directory.
# Since this test file is in 'tests/neurons', we go up two levels ('..', '..').
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

# Now we can import the module we want to test from the 'neurons' package.
from neurons import target_provider

# Sample data for mocking function returns
MOCK_DYNAMIC_TARGETS = [{"id": "dynamic1"}, {"id": "dynamic2"}]
MOCK_CACHED_TARGETS = [{"id": "cached1"}]

@pytest.fixture(autouse=True)
def clean_cache_file():
    """A pytest fixture to ensure the cache file is removed before/after each test."""
    # The cache file is created in the root, so we reference it from there.
    cache_path = target_provider._CACHE_FILE_PATH
    if os.path.exists(cache_path):
        os.remove(cache_path)
    yield
    if os.path.exists(cache_path):
        os.remove(cache_path)

def test_tier1_success_path():
    """
    Verify the ideal path: Tier 1 succeeds, data is returned, and cache is written.
    """
    # The path to patch must be the full path from the project root.
    with patch('neurons.target_provider._fetch_from_dynamic_source') as mock_fetch, \
         patch('neurons.target_provider._write_to_local_cache') as mock_write:

        mock_fetch.return_value = MOCK_DYNAMIC_TARGETS
        result = target_provider.get_scraping_targets()

        assert result == MOCK_DYNAMIC_TARGETS
        mock_fetch.assert_called_once()
        mock_write.assert_called_once_with(MOCK_DYNAMIC_TARGETS)

def test_tier1_fails_tier2_succeeds():
    """
    Verify the first fallback path: Tier 1 fails, Tier 2 succeeds.
    """
    with patch('neurons.target_provider._fetch_from_dynamic_source') as mock_fetch, \
         patch('neurons.target_provider._read_from_local_cache') as mock_read, \
         patch('neurons.target_provider._write_to_local_cache') as mock_write:

        mock_fetch.side_effect = IOError("Network is down")
        mock_read.return_value = MOCK_CACHED_TARGETS
        result = target_provider.get_scraping_targets()

        assert result == MOCK_CACHED_TARGETS
        mock_fetch.assert_called_once()
        mock_read.assert_called_once()
        mock_write.assert_not_called()

def test_tier1_and_tier2_fail_tier3_succeeds():
    """
    Verify the full fallback path: Tiers 1 and 2 fail, static data is returned.
    """
    with patch('neurons.target_provider._fetch_from_dynamic_source') as mock_fetch, \
         patch('neurons.target_provider._read_from_local_cache') as mock_read:

        mock_fetch.side_effect = IOError("Network is down")
        mock_read.return_value = None
        result = target_provider.get_scraping_targets()

        assert result == target_provider._STATIC_FALLBACK_TARGETS
        mock_fetch.assert_called_once()
        mock_read.assert_called_once()

def test_cache_is_actually_written_on_tier1_success():
    """
    An integration-style test to verify the file write actually happens.
    """
    with patch('neurons.target_provider._fetch_from_dynamic_source') as mock_fetch:
        mock_fetch.return_value = MOCK_DYNAMIC_TARGETS
        target_provider.get_scraping_targets()

        cache_path = target_provider._CACHE_FILE_PATH
        assert os.path.exists(cache_path)
        with open(cache_path, 'r') as f:
            data_from_disk = json.load(f)
        assert data_from_disk == MOCK_DYNAMIC_TARGETS

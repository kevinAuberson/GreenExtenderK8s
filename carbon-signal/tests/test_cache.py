"""
File:        test_cache.py
Author:      Kevin Auberson
Created:     2026-05-21
Description: Unit tests for the TTL cache module.
"""

import time

from cache import Cache


def test_set_and_get_returns_value():
    """A value stored in the cache can be retrieved before it expires."""
    c = Cache()
    c.set("my_key", "my_value", ttl_seconds=10)
    assert c.get("my_key") == "my_value"


def test_get_missing_key_returns_none():
    """Looking up a key that was never set returns None."""
    c = Cache()
    assert c.get("does_not_exist") is None


def test_get_expired_key_returns_none():
    """A value past its TTL is no longer returned."""
    c = Cache()
    c.set("my_key", "my_value", ttl_seconds=0)
    time.sleep(0.01)
    assert c.get("my_key") is None


def test_expired_key_is_deleted_from_store():
    """Expired entries are removed from the internal store on access."""
    c = Cache()
    c.set("my_key", "my_value", ttl_seconds=0)
    time.sleep(0.01)
    c.get("my_key")
    assert "my_key" not in c._store


def test_set_overwrites_existing_value():
    """Setting the same key twice keeps the latest value."""
    c = Cache()
    c.set("my_key", "old", ttl_seconds=10)
    c.set("my_key", "new", ttl_seconds=10)
    assert c.get("my_key") == "new"


def test_different_keys_are_independent():
    """Storing one key does not affect another."""
    c = Cache()
    c.set("key_a", "value_a", ttl_seconds=10)
    c.set("key_b", "value_b", ttl_seconds=10)
    assert c.get("key_a") == "value_a"
    assert c.get("key_b") == "value_b"


def test_value_can_be_any_type():
    """The cache stores arbitrary Python objects, not just strings."""
    c = Cache()
    c.set("dict_key", {"nested": [1, 2, 3]}, ttl_seconds=10)
    assert c.get("dict_key") == {"nested": [1, 2, 3]}

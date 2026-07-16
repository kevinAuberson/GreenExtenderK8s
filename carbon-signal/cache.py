"""
File:           cache.py
Author:         Kevin Auberson
Created:        2026-05-10
Description:    In-memory TTL cache used by all collectors to avoid hammering external APIs
                on every request.
"""

import time


class Cache:
    """Simple TTL cache backed by a dictionary."""

    def __init__(self):
        self._store = {}

    def get(self, key):
        """
        Retrieve a value from the cache.

        Args:
            key: The cache key to look up.

        Returns:
            The cached value if present and not expired, otherwise None.
        """
        if key not in self._store:
            return None
        value, expires_at = self._store[key]
        if time.time() > expires_at:
            del self._store[key]
            return None
        return value

    def set(self, key, value, ttl_seconds):
        """
        Store a value in the cache with a given lifetime.

        Args:
            key: the cache key
            value: the value to store.
            ttl_seconds: How long the value stays valid, in seconds.
        """
        self._store[key] = (value, time.time() + ttl_seconds)


# Shared instance imported by all modules
cache = Cache()

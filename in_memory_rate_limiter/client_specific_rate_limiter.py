"""
A thread-safe, in-memory, token bucket–style rate limiter that supports the following operations:

Functional Requirements:
    allow_request(client_id: str) -> bool
    Returns True if the client is allowed to make a request at this moment, otherwise False.

Each client has a configuration:
    max_tokens (bucket size)
    refill_rate (tokens per second)

When a request arrives:
    Refill tokens based on elapsed time since last refill.
    If a token is available → consume & allow.
    Otherwise → deny.

The service must work correctly under high concurrency (multiple threads calling allow_request() for same or different clients).
"""
import time
import threading


class ThreadSafeInMemoryRateLimiter:
    def __init__(self):
        # For each client, we have a tuple with the following:
        #   1. The refill rate (tokens per second)
        #   2. The 'monotonic' timestamp when the tokens available was updated
        #   3. Tokens available (as of the timestamp in (2)).
        self.client_map = {
            "dummy_client_id": (1, time.monotonic(), 60)
        }
        self.lock = threading.lock()

    def allow_request(client_id: str, cost:int = 1) -> bool:
        ...
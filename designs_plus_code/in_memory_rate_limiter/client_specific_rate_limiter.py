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
from time import sleep

from interfaces import RateLimiter
import logging

class ClientState:
    # For each client, we maintain the following:
    #   1. The max tokens allowed for the client.
    #   2. The refill rate (tokens per second)
    #   3. The 'monotonic' timestamp when the tokens available was updated
    #   4. Tokens available (as of the timestamp in (3)).
    def __init__(
            self,
            max_tokens: int,
            refill_rate: int,
            timestamp: float,
            available_tokens: int
    ):
        self.max_tokens = max_tokens
        self.refill_rate = refill_rate
        self.last_updated = timestamp
        self.last_available_tokens = available_tokens

class ThreadSafeInMemoryRateLimiter(RateLimiter):
    def __init__(self):
        # For each client, we are maintaining a tuple with the client's state and a lock, specific to the client
        self._client_map = {
            "dummy_client_id": (
                ClientState(
                    120,
                    1,
                    time.monotonic(),
                    120
                ),
                threading.Lock()
            ) #Adding a dummy client during init.
        }
        #A system level lock to protect operations across the dict (like adding clients)
        self._lock = threading.Lock()

    def allow_request(self, client_id: str, cost:int = 1) -> bool:
        # ToDo: Add docstrings

        if client_id in self._client_map:
            client_state, client_lock = self._client_map[client_id]
        else:
            raise Exception(f"Client {client_id} is not onboarded.")

        client_lock.acquire()
        try:
            max_tokens = client_state.max_tokens
            refill_rate = client_state.refill_rate
            last_updated = client_state.last_updated
            last_available_tokens = client_state.last_available_tokens
            new_ts = time.monotonic()
            available_tokens = min(
                int(last_available_tokens + ((new_ts - last_updated) * refill_rate)),
                max_tokens
            )
            if available_tokens >= cost:
                #Update the client's state before returning true
                new_available_tokens = available_tokens - cost
                self._client_map[client_id] = (
                    ClientState(
                        max_tokens,
                        refill_rate,
                        new_ts,
                        new_available_tokens
                    ),
                    client_lock
                )
                return True
            else:
                logging.info(f"Available tokens for client ({available_tokens}) less than cost ({cost})")
                return False
        except Exception as e:
            logging.error(f"An unexpected error occurred. Details: \n\t{e}")
            raise e
        finally:
            client_lock.release()

    def add_client(self, client_id: str, max_tokens: int = 60, refill_rate: int = 1):
        # ToDo: Add docstrings
        self._lock.acquire()
        try:
            if client_id in self._client_map:
                raise Exception(f"Client {client_id} is already onboarded.")
            client_lock = threading.Lock()
            self._client_map[client_id] = (
                ClientState(
                    max_tokens,
                    refill_rate,
                    time.monotonic(),
                    max_tokens
                ),
                client_lock
            )
        except Exception as e:
            logging.error(f"An error occurred. Details: \n\t{e}")
            raise e
        finally:
            self._lock.release()


if __name__ == "__main__":
    rate_limiter = ThreadSafeInMemoryRateLimiter()
    rate_limiter.add_client("client_random", 60, 1)
    #rate_limiter.add_client("client_random", 60, 1) # Will throw an exception
    client = "client_1"
    rate_limiter.add_client(client, 2, 1)
    assert not rate_limiter.allow_request(client, 3)    # Available tokens is 2, will not allow
    assert rate_limiter.allow_request(client, 1)
    assert rate_limiter.allow_request(client, 1)
    assert not rate_limiter.allow_request(client, 1) # Two initial tokens are now used up, will not allow
    sleep(1)    # One token gets refilled
    assert rate_limiter.allow_request(client, 1)    # One token available after sleep. Will allow

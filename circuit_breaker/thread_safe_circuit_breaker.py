"""
A generic Circuit Breaker wrapper around a callable (e.g., HTTP request, RPC call, DB query).
This circuit breaker protects the system from hammering a failing downstream service.

Three States

CLOSED
* Normal operation.
* All calls allowed.
* Track failures.
* If failures exceed a threshold, switch to OPEN.

OPEN
* Calls should fail fast (do NOT execute the underlying function).
* After reset_timeout seconds, move to HALF-OPEN.

HALF-OPEN
* Allow a single test call through.
* If it succeeds → transition back to CLOSED and reset failure count.
* If it fails → transition back to OPEN and restart the cooldown timer.

"""
import logging
import subprocess
import threading
import time
from enum import Enum
from abc import ABC, abstractmethod
from threading import Thread

import requests
from flask import Response, make_response
from requests.exceptions import HTTPError, ConnectionError, Timeout, ConnectTimeout, TooManyRedirects
import sys


logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)

class CIRCUIT_BREAKER_STATE(Enum):
    CLOSED = 0
    HALF_OPEN = 1
    OPEN = 2


class RESPONSE_TYPE(Enum):
    OK = 0
    SERVER_ERROR = 1 # Only 5XXs will for now be handled separately


class GetAPICircuitBreaker:
    def __init__(self, endpoint: str, failure_threshold, reset_timeout, request_timeout = 5):     # Timeout: 5 seconds by default

        # Mutable State variables
        self.state = CIRCUIT_BREAKER_STATE.CLOSED
        self.first_failure_ts = None
        self.circuit_breaker_open_ts = None
        self.num_failures = 0

        #Immutable state variables
        self.failure_threshold = failure_threshold  # Number of failures before the circuit is opened.
        self.reset_timeout = reset_timeout          # Seconds elapsed before the circuit half-opened from open
        self.request_timeout = request_timeout      # Timeout for each get request, before marking it as failed
        self.api_base_url = endpoint                #
        self.lock = threading.Lock()
        self.health_endpoint = "health" # Hardcoding for now

        Thread(target = self.watcher).start()   # Starting a watcher to keep track of the seconds elapsed in open state


    def call(self, api_endpoint: str, args: dict) -> Response:
        with self.lock:
            if self.state == CIRCUIT_BREAKER_STATE.CLOSED:
                try:
                    # Allow call
                    response = requests.get(self.api_base_url + "/" + api_endpoint, json=args, timeout=self.request_timeout)
                    response.raise_for_status()
                    # If flow reaches this point, the request passed.
                    #   update first_failure_ts to None
                    #   set num_failures to zero
                    self.first_failure_ts = None
                    self.num_failures = 0
                    logging.debug("Request passed.")
                    self.log_state_variables()
                    return response
                # If call fails:
                #   increment num_failures
                #   if first_failure_ts is None:
                #       set first_failure_ts to current timestamp
                #   If num_failures >= failure_threshold:
                #       change state to OPEN
                #       set circuit_breaker_open_ts to current timestamp
                except (HTTPError, ConnectionError, Timeout, ConnectTimeout, TooManyRedirects) as e:
                    self.num_failures+=1
                    current_time = time.monotonic()
                    if self.first_failure_ts is None:
                        self.first_failure_ts = current_time
                    if self.num_failures >= self.failure_threshold:
                        self.state = CIRCUIT_BREAKER_STATE.OPEN
                        self.circuit_breaker_open_ts = current_time
                    logging.debug("Request failed.")
                    logging.debug(
                        "Current state variables:"
                        f"  state: {self.state}" +
                        f"  first_failure_ts: {self.first_failure_ts}\n"
                        f"  circuit_breaker_open_ts: {self.circuit_breaker_open_ts}\n"
                        f"  num_failures: {self.num_failures}\n"
                    )
                    #return make_response(f"Request failed. Reason: {type(e).__name__}", 500)
                    return None
                except Exception as e:
                    raise Exception("Unhandled exception occured. Details: "+e)


            elif self.state == CIRCUIT_BREAKER_STATE.OPEN:
                # Fail request
                #return make_response(f"Request failed. Reason: Circuit Open", 500)
                return None

            elif self.state == CIRCUIT_BREAKER_STATE.HALF_OPEN:
                try:
                    # Allow call
                    # If call fails:
                    #   Change state to open
                    #   set circuit_breaker_open_ts to current timestamp
                    #   increment num_failures
                    response = requests.get(self.api_base_url + "/" + api_endpoint, json=args, timeout=self.request_timeout)
                    response.raise_for_status()
                    # If the flow reaches this point, the request passed.
                    self.state = CIRCUIT_BREAKER_STATE.CLOSED
                    self.first_failure_ts = None
                    self.circuit_breaker_open_ts = None
                    self.num_failures = 0
                    logging.debug("Request passed while on Half Open. Circuit is now closed.")
                    self.log_state_variables()
                    return response

                except (HTTPError, ConnectionError, Timeout, ConnectTimeout, TooManyRedirects) as e:
                    logging.debug("Request failed while on Half Open. Circuit is now open again.")
                    self.state = CIRCUIT_BREAKER_STATE.OPEN
                    current_time = time.monotonic()
                    self.circuit_breaker_open_ts = current_time
                    self.num_failures+=1

                    self.log_state_variables()
                    #return make_response("Request failed. Circuit open", 500)
                    return None
                except Exception as e:
                    raise Exception("Unhandled exception occured. Details: "+e)
                # Else:
                #   Change state to closed
                #   Set first_failure_ts to None
                #   Set num_failures to zero
                #   Set circuit_breaker_open_ts to None
            else:
                raise Exception("Unhandled exception. Circuit breaker is in an unknown state.")

    def log_state_variables(self):
        logging.debug(
            "Current state variables:\n"
            f"  state: {self.state}\n" +
            f"  first_failure_ts: {self.first_failure_ts}\n"
            f"  circuit_breaker_open_ts: {self.circuit_breaker_open_ts}\n"
            f"  num_failures: {self.num_failures}\n"
        )

    def watcher(self):
        while True:
            with self.lock:
                if self.state == CIRCUIT_BREAKER_STATE.OPEN:
                    # evaluate seconds_open = current_ts - circuit_breaker_open_ts
                    # if  seconds_open >= reset_timeout
                    #   Change state to HALF_OPEN
                    current_time = time.monotonic()
                    seconds_open = current_time - self.circuit_breaker_open_ts
                    logging.debug(f"Second Elapsed since circuit open: {seconds_open}")
                    if seconds_open >= self.reset_timeout:
                        logging.debug("Reset timeout reached. Moving circuit state to half open.")
                        self.state = CIRCUIT_BREAKER_STATE.HALF_OPEN # Moving from open to half open
            time.sleep(1) # Check every second


class CircuitOpenException(Exception):
    def __init__(self, e):
        super().__init__(f"Request Blocked. Circuit open.\n{e}")

if __name__ == "__main__":

    # Run server from terminal

    cb = GetAPICircuitBreaker(
        endpoint="http://localhost:8019",
        failure_threshold=5,        # After 5 consecutive failures, change state to open
        reset_timeout=10,           # after 10 seconds in the open state, move to half open
        request_timeout=2           # Timeout of 2 seconds for each request
    )

    time.sleep(2)

    assert cb.call("/greet", {}).status_code == 200
    assert cb.state == CIRCUIT_BREAKER_STATE.CLOSED
    cb.call("/greetAfterSleep", {})       # Should timeout
    cb.call("/greetAfterSleep", {})       # Should timeout
    cb.call("/greetAfterSleep", {})       # Should timeout
    cb.call("/greetAfterSleep", {})       # Should timeout
    cb.call("/greetAfterSleep", {})       # Should timeout
    cb.state == CIRCUIT_BREAKER_STATE.OPEN # Should open circuit after 5 failures
    time.sleep(12) # After this sleep, the watcher daemon should change the state to Half open
    assert cb.state == CIRCUIT_BREAKER_STATE.HALF_OPEN
    cb.call("/greetAfterSleep", {})       # Should timeout
    assert cb.state == CIRCUIT_BREAKER_STATE.OPEN               # Open again after the above failure
    time.sleep(12)  # After this sleep, the watcher daemon should change the state to Half open
    assert cb.state == CIRCUIT_BREAKER_STATE.HALF_OPEN
    assert cb.call("/greet", {}).status_code == 200     # First request when half open. Should pass.
    assert cb.state == CIRCUIT_BREAKER_STATE.CLOSED   # Should now be closed

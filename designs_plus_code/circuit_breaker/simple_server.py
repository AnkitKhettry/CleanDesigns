"""
A simple flask server that accepts get requests, accepts a <name> as query param and responds with "Hello <name>!"
"""
import time
from flask import Flask, request, make_response

hello_app = Flask("HelloApp")

@hello_app.route("/greet", methods=["GET"])
def greet():
    params = request.args
    name = params.get("name", "anonymous")
    return make_response(f"Hello {name}!", 200)

@hello_app.route("/greetAfterSleep", methods=["GET"])
def greet_after_sleep():
    params = request.args
    name = params.get("name", "anonymous")
    time.sleep(5) # Sleep 5 seconds before greeting
    return make_response(f"Hello {name}!", 200)

@hello_app.route("/health", methods=["GET"])
def health():
    return make_response("OK", 200)

if __name__ == "__main__":
    hello_app.run(port = 8019, debug=False, use_reloader=False)
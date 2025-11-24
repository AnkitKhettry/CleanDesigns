"""
An in-memory, thread-safe, multi-producer, multi-subscriber message queue with the following features:

1. Queue operations
    A class MessageQueue with:
        publish(topic: str, message: str) -> None
            Adds a message to a topic.
        subscribe(topic: str) -> Subscriber
            Returns a Subscriber object that can consume messages from that topic.
        Subscriber.poll(timeout: Optional[float] = None) -> Optional[str]
            Blocking call: waits for the next message, or returns None on timeout.

2. Behavior
    * Multiple threads may publish to the same or different topics.
    * Multiple subscribers may subscribe to the same topic — each subscriber maintains its own cursor, like Kafka.
    * poll() should block efficiently, not busy-wait.

No message loss — all subscribers receive all messages (independent cursors).
"""
import asyncio
import random
import threading
import time
import logging
import sys

logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)

class Topic:
    def __init__(self, name: str, max_size: int = 10):
        self.name = name
        self.queue = []
        self.max_size = max_size
        self.first_offset = -1
        self.last_offset = -1

    def publish(self, message: str):
        self.queue.append((message, time.monotonic())) # Message string and the time of consumption, to be used later for TTL based deletion
        if self.first_offset == -1:
            self.first_offset = 0
        self.last_offset += 1

    def fetch(self, offset: int):
        if offset <= self.last_offset:
            return self.queue[(offset - self.first_offset + 1):], self.last_offset
        else:
            raise Exception(f"Client offset {offset} is out of range")

    # 0, 1, 2, [3, 4, 5, 6, 7, 8, 9, 10, 11, 12] - 10 messages in queue, 3 deleted
    # last offset: 12
    # First offset: 3
    # client offset: 8
    # queue[8 - 3 + 1:]

    def poll(self, offset: int):
        return self.last_offset > offset

    def delete_messages(self):
        # Delete messages that are older than a certain timestamp
        raise NotImplementedError()


class MessageQueue:
    #Interface for users
    def __init__(self):
        self._topics = {} #topic_id: (Topic, lock)
        self._producers = {} #topic_id: [subscriber_ids]
        self._subscribers = {} #topic_id: [producer_ids]

    def add_topic(self, topic_id: str, topic_name: str):
        if topic_id not in self._topics:
            topic = Topic(topic_name)
            topic_lock = threading.Lock()
            self._topics[topic_id] = (topic, topic_lock)
            self._producers[topic_id] = set()
            self._subscribers[topic_id] = set()
        else:
            raise Exception(f"Error: {topic_id} already exists.")

    def add_producer(self, topic_id: str, producer_id: str):
        if topic_id not in self._topics:
            raise Exception(f"Error: {topic_id} does not exist.")
        self._producers[topic_id].add(producer_id)

    def add_subscriber(self, topic_id: str, subscriber_id: str):
        if topic_id not in self._topics:
            raise Exception(f"Error: {topic_id} does not exist.")
        self._subscribers[topic_id].add(subscriber_id)

    def publish_message(self, topic_id: str, message:str, producer_id: str):
        if not topic_id in self._topics:
            raise Exception(f"Error: {topic_id} does not exist.")

        if not producer_id in self._producers[topic_id]:
            raise Exception(f"Producer {producer_id} is not onboarded.")

        topic, topic_lock = self._topics[topic_id]
        with topic_lock:
            topic.publish(message)

    def consume_messages(self, topic_id: str, offset: int,  subscriber_id: str):
        if not topic_id in self._topics:
            raise Exception(f"Error: {topic_id} does not exist.")

        if not subscriber_id in self._subscribers[topic_id]:
            raise Exception(f"Subscriber {subscriber_id} is not onboarded.")

        topic, topic_lock = self._topics[topic_id]
        with topic_lock:
            messages_with_timestamps, new_offset =  topic.fetch(offset)
            return messages_with_timestamps, new_offset

    def poll_topic(self, topic_id: str, offset: int):
        if not topic_id in self._topics:
            raise Exception(f"Error: {topic_id} does not exist.")
        topic, topic_lock = self._topics[topic_id]
        with topic_lock:
            return topic.poll(offset)

    def cleanup_topics(self):
        raise NotImplementedError()


class Producer:
    def __init__(self, id: str, topic_id: str, queue: MessageQueue):
        self.id = id
        self.topic_id = topic_id
        self.queue = queue
        self.queue.add_producer(topic_id, id)

    async def produce_sequential_integers(self):
        for i in range(10):
            await asyncio.sleep(random.randint(1,10)) # Sleep upto 10 seconds before producing again
            message_to_produce = str(i)
            logging.debug(f"Producer: {self.id}. Message produced: {message_to_produce} to topic {self.topic_id}")
            self.queue.publish_message(self.topic_id, message_to_produce, self.id)

    async def produce_random_integers(self):
        for i in range(10):
            message_to_produce = str(random.randint(1, 100))
            await asyncio.sleep(random.randint(1,10)) # Sleep upto 10 seconds before producing again
            logging.debug(f"Producer: {self.id}. Message produced: {message_to_produce} to topic {self.topic_id}")
            self.queue.publish_message(self.topic_id, message_to_produce, self.id)


class Subscriber:
    def __init__(self, id: str, topic_id: str, queue: MessageQueue):
        self.id = id
        self.topic_id = topic_id
        self.queue = queue
        self.offset = -1
        self.queue.add_subscriber(topic_id, id)
        self.backoff = 1 # Sleep these many seconds before polling again.

    async def consume_messages(self):
        while True:
            await asyncio.sleep(self.backoff)
            if self.queue.poll_topic(self.topic_id, self.offset):
                messages, new_offset = self.queue.consume_messages(self.topic_id, self.offset, self.id)
                self.offset = new_offset
                for message, ts in messages:
                    logging.debug(f"Consumer: {self.id}. Message consumed: {message}")
                self.backoff = 1 # Resetting backoff
            else:
                logging.debug(f"Subscriber {self.id}: No new messages produced in topic {self.topic_id}. Waiting {self.backoff} second(s).")
                self.backoff = min(self.backoff*2, 300) # Cap at 5 mins.


async def main():
    mq = MessageQueue()

    mq.add_topic("T1", "Random Numbers")
    mq.add_topic("T2", "Sequential Numbers")

    producer1 = Producer("P1", "T1", mq)
    producer2 = Producer("P2", "T2", mq)

    subscriber1 = Subscriber("S1", "T1", mq)
    subscriber2 = Subscriber("S2", "T1", mq)
    subscriber3 = Subscriber("S3", "T2", mq)
    subscriber4 = Subscriber("S4", "T2", mq)

    await asyncio.gather(
        subscriber1.consume_messages(),
        subscriber2.consume_messages(),
        subscriber3.consume_messages(),
        subscriber4.consume_messages(),
        producer1.produce_sequential_integers(),
        producer2.produce_random_integers()

    )

if __name__ == "__main__":
    asyncio.run(main())

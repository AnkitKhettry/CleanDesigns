import threading
from abc import ABC, abstractmethod
from time import sleep
import random

from designs_plus_code.job_scheduler.constants import JOB_STATES

#ToDo: Not ready. Several concurrency issues to be revisited.

class Callable(ABC):
    @abstractmethod
    def call(self, **args):
        pass

    def is_pauseable(self):
        return False

    def is_cancelable(self):
        return False

    def get_state(self):
        return None

class Pauseable(ABC):
    @abstractmethod
    def pause(self):
        pass

class Cancelable(ABC):
    @abstractmethod
    def cancel(self):
        pass

class VerySimpleJob(Callable, Cancelable):

    """
    Accepts 'num_threads' as a parameter. Spawns that many number of thread.
    Each thread prints a random number every second for five seconds, unless the job is cancelled.
    """

    def __init__(self, **kwargs):
        print(f"VerySimpleJob is initialized with kwargs:{kwargs}")
        self.lock = threading.Lock()
        self.isCancelable = True
        self.isPauseable = False
        num_threads = kwargs.get("num_threads")
        self.threads = [threading.Thread(target=self.return_random_integer, args=(i,)) for i in range(num_threads)]
        self.state = JOB_STATES.INITIALIZED
        self.cancel_event = threading.Event()
        self.watch_thread = threading.Thread(target=self.watch)


    def watch(self):
        for thread in self.threads:
            thread.join()
        with self.lock:
            if self.state not in [JOB_STATES.CANCELLED, JOB_STATES.FAILED]:
                self.state = JOB_STATES.COMPLETED

    def return_random_integer(self, id):
        try:
            print(f"Thread {id} is running")
            i = 0
            while i<5 and not self.cancel_event.is_set():
                sleep(1)
                i+=1
                print(f"Here's a random number from thread {id}: {random.randint(1, 10)}")
            print(f"Thread {id} is completed")
        except Exception as e:
            print(f"Thread {id} is terminated due to {e}")
            with self.lock:
                self.state = JOB_STATES.FAILED

    def is_cancelable(self):
        return self.isCancelable

    def is_pauseable(self):
        return self.isPauseable

    def call(self, **args):
        self.state = JOB_STATES.RUNNING
        for thread in self.threads:
            thread.start()
        self.watch_thread.start()


    def get_state(self):
        with self.lock:
            return self.state

    def cancel(self):
        self.cancel_event.set()
        with self.lock:
            self.state = JOB_STATES.CANCELLED

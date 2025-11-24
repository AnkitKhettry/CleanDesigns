import threading
import uuid
from abc import ABC, abstractmethod
from threading import Thread


class Scheduler(ABC):

    @abstractmethod
    def run(self, callable, kwargs):
        pass

    @abstractmethod
    def schedule(self, callable):
        pass

    @abstractmethod
    def cancel(self, job_id):
        pass

    @abstractmethod
    def pause(self, job_id):
        pass

    @abstractmethod
    def describe_jobs(self):
        pass

    @abstractmethod
    def get_job_status(self, job_id):
        pass

    @abstractmethod
    def get_job_result(self, job_id):
        pass

    @abstractmethod
    def get_job_logs(self, job_id):
        pass


class SimpleScheduler(Scheduler):

    def __init__(self):
        self.lock = threading.Lock()
        self.jobs = {}

    def run(self, callable, kwargs):
        print(f"Callable {callable.__class__} is scheduled with kwargs: {kwargs}")
        job_id = uuid.uuid4()
        job = callable(**kwargs)
        with self.lock:
            self.jobs[job_id] = job
        Thread(target=job.call, daemon=True).start()
        return job_id

    def schedule(self, callable):
        # TODO: Later.
        pass

    def cancel(self, job_id):
        with self.lock:
            if self.jobs[job_id].is_cancelable():
                Thread(target = self.jobs[job_id].cancel, daemon=True).start()

    def pause(self, job_id):
        with self.lock:
            if self.jobs[job_id].is_pauseable():
                Thread(target = self.jobs[job_id].pause, daemon=True).start()

    def describe_jobs(self):
        for job_id, job in self.jobs.items():
            print(f"Job ID: {job_id}, Type: {job.__class__}, State: {job.get_state()}")

    def get_job_status(self, job_id):
        with self.lock:
            return self.jobs[job_id].get_state()

    def get_job_result(self, job_id):
        with self.lock:
            return self.jobs[job_id].get_state()

    def get_job_logs(self, job_id):
        pass


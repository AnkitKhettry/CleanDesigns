import unittest
from threading import Thread
from time import sleep

from designs_plus_code.job_scheduler.constants import JOB_STATES
from designs_plus_code.job_scheduler.jobs import VerySimpleJob
from designs_plus_code.job_scheduler.schedulers import SimpleScheduler


class SimpleSchedulerTests(unittest.TestCase):
    scheduler = SimpleScheduler()

    def test_scheduler_happycase(self):
        job_id = self.scheduler.run(VerySimpleJob, {"num_threads": 3})
        probe_thread = Thread(target=self.probe_job_status, args=(job_id,))
        probe_thread.start()
        probe_thread.join()

        assert(self.scheduler.get_job_status(job_id) == JOB_STATES.COMPLETED)


    def test_scheduler_cancelled(self):
        job_id = self.scheduler.run(VerySimpleJob, {"num_threads": 3})
        probe_thread = Thread(target=self.probe_job_status, args=(job_id,))
        probe_thread.start()
        #Cancel the job after three seconds
        sleep(3)
        self.scheduler.cancel(job_id)
        probe_thread.join()

        #NOTE: In MOST cases, the job should be cancelled after 3 seconds. However, the outcome is not deterministic.

        assert(self.scheduler.get_job_status(job_id) == JOB_STATES.CANCELLED)

    def probe_job_status(self, job_id):
        while True and self.scheduler.get_job_status(job_id) in [JOB_STATES.INITIALIZED, JOB_STATES.RUNNING]:
            print(f"Job status: {self.scheduler.get_job_status(job_id)}")
            sleep(1)


if __name__ == '__main__':
    unittest.main()

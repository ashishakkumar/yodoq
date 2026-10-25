import base64
import json
import os
import tempfile
from dataclasses import dataclass
from typing import Dict, Optional


@dataclass
class LeasedJob:
    """What a worker receives when it successfully leases a job."""
    id: str
    payload: bytes
    lease_expires_at: int  # milliseconds, in the same frame as Clock.now()


@dataclass
class Stats:
    """Reports the current size of each job state."""
    pending: int = 0    # never leased, or failed/expired and waiting
    leased: int = 0     # currently held by a worker (active lease)
    completed: int = 0  # acked successfully
    exhausted: int = 0  # pending visit at max_attempts; not pending or leased
    leased_jobs: tuple = ()  # (id, worker_id, lease_expires_at, attempts)


@dataclass
class JobRecord:
    """Internal state for a single job execution."""
    id: str
    payload: bytes
    status: str = "PENDING"       # PENDING -> LEASED -> COMPLETED, or EXHAUSTED
    worker_id: str = ""           # Current lease holder
    lease_expires_at: int = 0     # Timestamp (ms) when the lease expires
    attempts: int = 0             # Retry counter


class JobQueue:
    """
    Single-process job queue with crash recovery.
    In-memory unless path is set. Attempts are uncapped unless max_attempts is set.
    """

    def __init__(self, clock, max_attempts=None, path=None) -> None:
        self.clock = clock
        self.max_attempts = max_attempts
        self.path = path # this is the path to the file that stores the queue : means that the queue is persistent
        self.jobs: Dict[str, JobRecord] = {} # this is the dictionary that stores the jobs
        self.next_id: int = 0 # this is the next id for the job , we need this because we want to generate unique ids for the jobs
        self._load() # this is the function that loads the queue from the  file that is stored in the path for persistent storage

    def _reclaim_expired_leases(self) -> None:
        """this function is used to reclaim expired leases , 
        it is used to move the jobs that are expired back to the pending state
         so that they can be leased to another worker
        """
        now = self.clock.now()
        moved = False
        for job in self.jobs.values():
            if job.status == "LEASED" and now >= job.lease_expires_at:
                job.status = "PENDING"
                job.worker_id = ""
                job.lease_expires_at = 0
                moved = True
        if moved:
            self._persist()

    def _persist(self) -> None: 
        if self.path is None: #when can this happen in real world scenario ? if the path is not set then the queue is not persistent
            return
        blob = {
            "next_id": self.next_id,
            "jobs": [
                {
                    "id": job.id,
                    "payload": base64.b64encode(job.payload).decode("ascii"),
                    "status": job.status,
                    "worker_id": job.worker_id,
                    "lease_expires_at": job.lease_expires_at,
                    "attempts": job.attempts,
                }
                for job in self.jobs.values()
            ],
        }
        directory = os.path.dirname(self.path) or "."
        fd, tmp = tempfile.mkstemp(dir=directory)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(blob, fh)
            os.replace(tmp, self.path)
        except Exception:
            if os.path.exists(tmp):
                os.unlink(tmp)
            raise

    def _load(self) -> None:
        if self.path is None or not os.path.isfile(self.path):
            return
        if os.path.getsize(self.path) == 0:
            return
        with open(self.path, encoding="utf-8") as fh:
            data = json.load(fh)
        self.next_id = data["next_id"]
        self.jobs = {}
        released = False
        for raw in data["jobs"]:
            job = JobRecord(
                id=raw["id"],
                payload=base64.b64decode(raw["payload"]),
                status=raw["status"],
                worker_id=raw["worker_id"],
                lease_expires_at=raw["lease_expires_at"],
                attempts=raw["attempts"],
            )
            if job.status == "LEASED":
                job.status = "PENDING"
                job.worker_id = ""
                job.lease_expires_at = 0
                released = True
            self.jobs[job.id] = job
        if released:
            self._persist()

    def enqueue(self, payload: bytes) -> str:
        """Stores a new job in PENDING state and returns its unique ID."""
        self.next_id += 1
        job_id = f"job-{self.next_id}"

        self.jobs[job_id] = JobRecord(
            id=job_id,
            payload=bytes(payload),
            status="PENDING",
        )
        self._persist()
        return job_id

    def lease(self, worker_id: str, lease_ms: int) -> Optional[LeasedJob]:
        """
        Atomically reserves the next available PENDING job for `worker_id`
        for `lease_ms` milliseconds. Returns None when no job is available.
        """

       
        self._reclaim_expired_leases()

        for job_id, job in list(self.jobs.items()):
            if job.status != "PENDING":
                continue
            # The lease that reaches max_attempts still runs. A later PENDING visit stops.
            if self.max_attempts is not None and job.attempts >= self.max_attempts:
                job.status = "EXHAUSTED"
                self._persist()
                continue

            job.status = "LEASED"
            job.worker_id = worker_id
            job.lease_expires_at = self.clock.now() + lease_ms
            job.attempts += 1

            # Move to the back of the dict for fair FIFO round-robin ordering
            del self.jobs[job_id]
            self.jobs[job_id] = job
            self._persist()

            return LeasedJob(
                id=job.id,
                payload=job.payload,
                lease_expires_at=job.lease_expires_at,
            )

        return None

    def ack(self, worker_id: str, job_id: str) -> None:
        """
        Marks a job as COMPLETED. Raises ValueError if `worker_id` is not
        the current active lease holder (e.g. lease expired or reassigned).
        """
        self._reclaim_expired_leases()

        job = self.jobs.get(job_id)
        if job is None:
            raise ValueError(f"Job {job_id} not found")

        if job.status != "LEASED" or job.worker_id != worker_id:
            raise ValueError(
                f"Stale Ack rejected: worker {worker_id} does not hold active lease for {job_id}"
            )

        job.status = "COMPLETED"
        job.worker_id = ""
        job.lease_expires_at = 0
        self._persist()

    def fail(self, worker_id: str, job_id: str) -> None:
        """
        Releases the lease without completing. The job immediately becomes
        re-leasable (PENDING).
        """
        self._reclaim_expired_leases()

        job = self.jobs.get(job_id)
        if job is None:
            raise ValueError(f"Job {job_id} not found")

        if job.status != "LEASED" or job.worker_id != worker_id:
            raise ValueError(
                f"Invalid Fail: worker {worker_id} does not hold lease for {job_id}"
            )

        job.status = "PENDING"
        job.worker_id = ""
        job.lease_expires_at = 0
        self._persist()

    def stats(self) -> Stats:
        """Returns counts of jobs by state."""
        self._reclaim_expired_leases()

        s = Stats()
        leased_jobs = []
        for job in self.jobs.values():
            if job.status == "PENDING":
                s.pending += 1
            elif job.status == "LEASED":
                s.leased += 1
                leased_jobs.append(
                    (job.id, job.worker_id, job.lease_expires_at, job.attempts)
                )
            elif job.status == "COMPLETED":
                s.completed += 1
            elif job.status == "EXHAUSTED":
                s.exhausted += 1
        s.leased_jobs = tuple(leased_jobs)
        return s


def New(clock, max_attempts=None, path=None) -> JobQueue:
    return JobQueue(clock, max_attempts=max_attempts, path=path)

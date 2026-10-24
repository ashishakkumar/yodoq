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


@dataclass
class JobRecord:
    """Internal state for a single job execution."""
    id: str
    payload: bytes
    status: str = "PENDING"       # State machine: PENDING -> LEASED -> COMPLETED
    worker_id: str = ""           # Current lease holder
    lease_expires_at: int = 0     # Timestamp (ms) when the lease expires
    attempts: int = 0             # Retry counter


class JobQueue:
    """
    Single-process, in-memory job queue with crash recovery and bounded duplication.
    """

    def __init__(self, clock) -> None:
        self.clock = clock
        self.jobs: Dict[str, JobRecord] = {}
        self.next_id: int = 0

    def _reclaim_expired_leases(self) -> None:
        """
        Transitions any LEASED jobs whose lease_expires_at <= clock.now()
        back to PENDING so another worker can pick them up.
        """
        now = self.clock.now()
        for job in self.jobs.values():
            if job.status == "LEASED" and now >= job.lease_expires_at:
                job.status = "PENDING"
                job.worker_id = ""
                job.lease_expires_at = 0

    def enqueue(self, payload: bytes) -> str:
        """Stores a new job in PENDING state and returns its unique ID."""
        self.next_id += 1
        job_id = f"job-{self.next_id}"

        self.jobs[job_id] = JobRecord(
            id=job_id,
            payload=bytes(payload),
            status="PENDING",
        )
        return job_id

    def lease(self, worker_id: str, lease_ms: int) -> Optional[LeasedJob]:
        """
        Atomically reserves the next available PENDING job for `worker_id`
        for `lease_ms` milliseconds. Returns None when no job is available.
        """
        self._reclaim_expired_leases()

        for job_id, job in list(self.jobs.items()):
            if job.status == "PENDING":
                job.status = "LEASED"
                job.worker_id = worker_id
                job.lease_expires_at = self.clock.now() + lease_ms
                job.attempts += 1

                # Move to the back of the dict for fair FIFO round-robin ordering
                del self.jobs[job_id]
                self.jobs[job_id] = job

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

    def stats(self) -> Stats:
        """Returns counts of jobs by state."""
        self._reclaim_expired_leases()

        s = Stats()
        for job in self.jobs.values():
            if job.status == "PENDING":
                s.pending += 1
            elif job.status == "LEASED":
                s.leased += 1
            elif job.status == "COMPLETED":
                s.completed += 1
        return s


def New(clock) -> JobQueue:
    return JobQueue(clock)

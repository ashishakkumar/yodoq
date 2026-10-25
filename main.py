import random
from typing import Dict, List
import solution


class TestClock:
    """Implements Clock with manual advancement in milliseconds."""
    def __init__(self) -> None:
        self._now: int = 0

    def now(self) -> int:
        return self._now

    def advance(self, delta_ms: int) -> None:
        self._now += delta_ms


class ExecutionLedger:
    """Out-of-band ground truth of how many times each job actually ran."""
    def __init__(self) -> None:
        self.counts: Dict[str, int] = {}

    def record(self, job_id: str) -> None:
        self.counts[job_id] = self.counts.get(job_id, 0) + 1

    def lost(self, all_ids: List[str]) -> int:
        return sum(1 for jid in all_ids if self.counts.get(jid, 0) == 0)

    def excess(self, all_ids: List[str]) -> int:
        return sum(
            self.counts[jid] - 1
            for jid in all_ids
            if self.counts.get(jid, 0) > 1
        )

    def total(self) -> int:
        return sum(self.counts.values())


class FailureSimulator:
    """Parameterizes how often workers crash or drop acks."""
    def __init__(
        self,
        crash_rate: float,
        ack_drop_rate: float,
        processing_time_ms: int,
        rng: random.Random,
    ) -> None:
        self.crash_rate = crash_rate
        self.ack_drop_rate = ack_drop_rate
        self.processing_time_ms = processing_time_ms
        self.rng = rng

    def should_crash(self) -> bool:
        return self.rng.random() < self.crash_rate

    def should_drop_ack(self) -> bool:
        return self.rng.random() < self.ack_drop_rate


class Worker:
    """Simulates a job consumer that may crash mid-process or drop acks."""
    def __init__(
        self,
        worker_id: str,
        queue: solution.JobQueue,
        clock: TestClock,
        ledger: ExecutionLedger,
        failures: FailureSimulator,
        lease_ms: int,
    ) -> None:
        self.id = worker_id
        self.queue = queue
        self.clock = clock
        self.ledger = ledger
        self.failures = failures
        self.lease_ms = lease_ms

    def tick(self) -> bool:
        try:
            job = self.queue.lease(self.id, self.lease_ms)
        except Exception:
            return False

        if job is None:
            return False

        self.clock.advance(self.failures.processing_time_ms)

        if self.failures.should_crash():
            return True

        self.ledger.record(job.id)

        if self.failures.should_drop_ack():
            return True

        try:
            self.queue.ack(self.id, job.id)
        except Exception:
            pass
        return True


def run_until_drained(
    q: solution.JobQueue, clock: TestClock, workers: List[Worker]
) -> None:
    max_iterations = 200_000
    for _ in range(max_iterations):
        any_progress = False
        for w in workers:
            if w.tick():
                any_progress = True

        clock.advance(2000)

        s = q.stats()
        if s.pending == 0 and s.leased == 0:
            return
        if not any_progress and s.pending == 0:
            return


def info(msg: str) -> None:
    print(f"[INFO] {msg}")

def successf(msg: str) -> None:
    print(f"[SUCCESS] ✓ {msg}")

def errf(msg: str) -> None:
    print(f"[ERROR] ✗ {msg}")

def warn(msg: str) -> None:
    print(f"[WARNING] ⚠ {msg}")

def section(title: str) -> None:
    print()
    print("=" * 60)
    print(f"📬 YODOQ {title.upper()}")
    print("=" * 60)

def intro() -> None:
    print("""
██╗   ██╗ ██████╗ ██████╗  ██████╗  ██████╗
╚██╗ ██╔╝██╔═══██╗██╔══██╗██╔═══██╗██╔═══██╗
 ╚████╔╝ ██║   ██║██║  ██║██║   ██║██║   ██║
  ╚██╔╝  ██║   ██║██║  ██║██║   ██║██║▄▄ ██║
   ██║   ╚██████╔╝██████╔╝╚██████╔╝╚██████╔╝
   ╚═╝    ╚═════╝ ╚═════╝  ╚═════╝  ╚══▀▀═╝

   Your Own Don't-Drop Queue (Python Edition)""")
    print("📬 Welcome to YODOQ — where workers crash and acks vanish.")
    print("💪 Let's see how reliable your queue really is...")


def level1() -> None:
    section("Level 1: Basic YODOQ Operations")

    clock = TestClock()
    q = solution.New(clock, path="jobs.json")

    info("Testing enqueue + lease + ack...")
    try:
        job_id = q.enqueue(b"hello")
        successf(f"Enqueued job {job_id}")
    except Exception as e:
        errf(f"Enqueue failed: {e}")
        return

    try:
        job = q.lease("worker-1", 5000)
        if job is None:
            errf("Lease failed: returned None")
            return
    except Exception as e:
        errf(f"Lease failed: {e}")
        return

    if job.payload != b"hello":
        errf("Leased job payload mismatch")
        return
    successf("Leased job with correct payload")

    try:
        q.ack("worker-1", job.id)
        successf("Acked job successfully")
    except Exception as e:
        errf(f"Ack failed: {e}")
        return

    if q.lease("worker-1", 5000) is None:
        successf("Acked job is not re-leasable")
    else:
        errf("Acked job should not be re-leasable")

    info("Testing multiple jobs...")
    for i in range(10):
        q.enqueue(f"job-{i}".encode())

    leased = 0
    while True:
        j = q.lease("worker-1", 5000)
        if j is None:
            break
        q.ack("worker-1", j.id)
        leased += 1

    if leased == 10:
        successf(f"Leased and acked all {leased} jobs")
    else:
        errf(f"Only leased {leased}/10 jobs")

    info("Testing fail re-lease...")
    q.enqueue(b"to-fail")
    j1 = q.lease("worker-1", 5000)
    if j1 is None:
        errf("Could not lease the job we just enqueued")
        return
    q.fail("worker-1", j1.id)
    j2 = q.lease("worker-2", 5000)
    if j2 is not None and j2.id == j1.id:
        successf("Failed job was re-leased to a different worker")
    else:
        errf("Failed job was not re-leased")
    if j2 is not None:
        q.ack("worker-2", j2.id)

    info("Testing stale ack rejection (lease expiry)...")
    q.enqueue(b"stale")
    s1 = q.lease("worker-1", 1000)
    if s1 is None:
        errf("Could not lease job for stale-ack test")
        return
    clock.advance(2000)
    s2 = q.lease("worker-2", 5000)
    if s2 is not None and s2.id == s1.id:
        try:
            q.ack("worker-1", s1.id)
            errf("Stale ack from expired lease holder was accepted (BAD)")
        except Exception:
            successf("Stale ack from expired lease holder was rejected")
        q.ack("worker-2", s2.id)
    else:
        warn("Lease did not transfer after expiry — Level 2 will surface this")


def level2() -> None:
    section("Level 2: YODOQ Worker Crash Recovery")

    clock = TestClock()
    q = solution.New(clock)
    ledger = ExecutionLedger()
    fails = FailureSimulator(
        crash_rate=0.20,
        ack_drop_rate=0.0,
        processing_time_ms=100,
        rng=random.Random(42),
    )

    job_count = 200
    info(f"Enqueuing {job_count} jobs...")
    job_ids: List[str] = [q.enqueue(f"job-{i}".encode()) for i in range(job_count)]

    workers = [
        Worker(f"worker-{i}", q, clock, ledger, fails, 1000)
        for i in range(5)
    ]

    info(f"Running 5 workers with {fails.crash_rate * 100:.0f}% crash rate...")
    run_until_drained(q, clock, workers)

    lost = ledger.lost(job_ids)
    excess = ledger.excess(job_ids)
    total = ledger.total()
    dup_rate = ((total / job_count) - 1) * 100 if job_count > 0 else 0.0

    info("Results:")
    info(f"  - Total executions:    {total}")
    info(f"  - Jobs lost (0 runs):  {lost} / {job_count}")
    info(f"  - Excess executions:   {excess}")
    info(f"  - Effective dup rate:  {dup_rate:.1f}%")

    if lost == 0:
        successf("No jobs were lost — every job ran at least once!")
    else:
        errf(f"{lost} jobs were lost forever (BAD).")

    if excess <= job_count * fails.crash_rate * 2:
        successf("Duplication is bounded reasonably given crash rate.")
    else:
        warn(f"High duplication: {excess} excess executions.")


def level3() -> None:
    section("Level 3: YODOQ Reliability Stress Sweep")

    rates = [0.001, 0.01, 0.05, 0.10, 0.20, 0.30, 0.50]
    job_count = 200

    rate_str = ", ".join(f"{r * 100:.1f}%" for r in rates)
    info(f"Sweeping crash + ack-drop rates: {rate_str}")

    for rate in rates:
        print()
        info(f"Testing rate: crash={rate * 100:.2f}%, ack-drop={rate * 100:.2f}%")

        clock = TestClock()
        q = solution.New(clock)
        ledger = ExecutionLedger()
        fails = FailureSimulator(
            crash_rate=rate,
            ack_drop_rate=rate,
            processing_time_ms=100,
            rng=random.Random(int(rate * 10000)),
        )

        job_ids: List[str] = [q.enqueue(f"job-{i}".encode()) for i in range(job_count)]
        workers = [
            Worker(f"worker-{i}", q, clock, ledger, fails, 1000)
            for i in range(5)
        ]

        run_until_drained(q, clock, workers)

        lost = ledger.lost(job_ids)
        excess = ledger.excess(job_ids)
        total = ledger.total()
        dup = ((total / job_count) - 1) * 100 if job_count > 0 else 0.0

        info("  Results:")
        info(f"    - Jobs lost:        {lost} / {len(job_ids)} ({lost / len(job_ids) * 100:.1f}%)")
        info(f"    - Excess execs:     {excess}")
        info(f"    - Duplication:      {dup:.1f}%")
        info(f"    - Total executions: {total}")

        if lost > 0:
            errf(f"Jobs lost at {rate * 100:.1f}% — reliability compromised.")


if __name__ == "__main__":
    intro()
    section("Reliability Assessment Protocol")
    level1()
    level2()
    level3()
    section("YODOQ Assessment Complete")

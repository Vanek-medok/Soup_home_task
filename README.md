# Job Queue Service

A PostgreSQL-backed job queue implemented in Go. The project focuses on reliable job processing under worker failures, duplicate requests, cancellation, and concurrent execution.

## Features

- Create jobs with an idempotency key.
- Retrieve a job by ID.
- List jobs with pagination.
- Cancel queued or running jobs.
- Process jobs using multiple workers.
- Recover jobs when a worker stops heartbeating and its lease expires.
- Prevent stale workers from committing results after losing ownership.
- Run a chaos harness that kills workers and checks the resulting database state.

## Architecture

The service consists of:

- **API** — accepts job creation, status, listing, and cancellation requests.
- **PostgreSQL** — stores jobs, their state, leases, and committed side effects.
- **Workers** — claim and process jobs concurrently.
- **Reaper** — detects expired leases and makes eligible jobs available for retry.
- **Chaos harness** — generates concurrent traffic and injects worker failures.

### Job lifecycle

```text
queued -> running -> succeeded
                  -> failed
                  -> cancelled
```

A job can be retried after a worker failure if its lease expires and the retry limit has not been reached.

## Reliability and consistency

### Atomic job claiming

Workers claim jobs inside a database transaction using PostgreSQL row locking with `FOR UPDATE SKIP LOCKED`. This prevents two workers from claiming the same queued job concurrently.

### Leases and recovery

A worker periodically sends heartbeats to extend its lease. If the worker dies or stops heartbeating, the lease expires. The reaper detects expired jobs and either requeues them or marks them failed when the retry limit is exhausted.

### Stale worker protection

Each claim increments the job's attempt/fencing value. Heartbeats and completion updates are conditional on the current attempt and running state. A worker whose lease has expired cannot commit a stale result.

### Side-effect guarantee

The system provides **at-least-once processing attempts** and **at-most-once committed database side effects per job**.

The side effect and transition to `succeeded` are committed in the same database transaction. A unique constraint on the effect's job reference prevents duplicate committed effects.

This does not provide exactly-once execution. If a job performs an external side effect, that external system must support idempotency or the integration must use an outbox-based design.

### Cancellation

Cancellation is coordinated through database state. A worker must still own the current job attempt to commit completion. This makes the completion/cancellation race resolve through conditional database updates rather than an unchecked in-memory decision.

## Why PostgreSQL?

PostgreSQL provides transactions, row-level locking, `SKIP LOCKED`, constraints, and durable state. These features allow job claiming, lease management, and side-effect commits to be coordinated in the database without introducing a separate coordination service.

## Running locally

The project uses Docker Compose.

```bash
docker compose up -d postgres migrate api
docker compose up -d --scale worker=8 worker
```

Check the running services:

```bash
docker compose ps
```

The exact Compose service names and environment variables are defined in `docker-compose.yml` and the example environment configuration.

## API

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/jobs` | Create a job |
| `GET` | `/jobs/{id}` | Get job status |
| `GET` | `/jobs?limit=&cursor=` | List jobs with pagination |
| `POST` | `/jobs/{id}/cancel` | Cancel a job |
| `GET` | `/healthz` | Health check |

Job creation requires an idempotency key. Repeating a request with the same key must not create another job.

## Chaos testing

The chaos harness drives the API and injects failures into worker processes. It supports configurable job count, concurrency, request rate, duplicate request rate, cancellation rate, worker kill interval, and worker freeze probability/duration.

Example:

```bash
docker compose run --rm chaos \
  -compose-project soup \
  -out /runs \
  -jobs 100 \
  -conc 16 \
  -rps 10 \
  -dup-rate 0.3 \
  -cancel-rps 0 \
  -kill-every 5s \
  -freeze-prob 0 \
  -reset
```

The harness checks properties including:

- Jobs are not lost.
- Idempotency keys do not create duplicate jobs.
- A job does not have multiple committed database effects.
- Stale attempts cannot commit effects.
- Final job states are consistent.
- Jobs do not remain stuck in `running`.
- Pagination and cancellation behavior remain consistent.

Raw logs and result files are kept under `runs/`. Both successful and failed runs should be retained so that the test history is auditable.

### Recorded test results

The latest successful run (`20261111T171909Z`) used 100 jobs, 16 concurrent request workers, 10 job-creation requests per second, a duplicate rate of 0.3, and worker process kills every 5 seconds. Cancellation and freeze injection were disabled for this run.

| Metric | Result |
|---|---:|
| Jobs in database | 100 |
| Succeeded | 100 |
| Committed effects | 100 |
| Unique jobs with effects | 100 |
| Duplicate effects | 0 |
| Reclaimed jobs | 12 |
| Maximum attempt | 2 |
| Worker kill actions | 14 |
| Duplicate POSTs | 71 |
| Unacknowledged idempotency keys in database | 0 |
| Harness result | PASS |

A separate recovery experiment also showed that expired jobs could be reclaimed after workers were restarted manually. That experiment should not be interpreted as proof of automatic recovery under every container-level failure mode.

Other run directories and their raw logs are retained in `runs/`. The results should be reviewed individually; a `PASS` from a run that did not exercise a particular failure mode is not evidence that the failure mode is covered.

## Known limitations and risks

- The latest successful chaos run covers 100 jobs at 10 RPS, not the assignment target of 10,000 jobs and 100 RPS.
- The latest successful run did not inject cancellations or worker freezes.
- The harness cannot prove correctness for every possible interleaving or database failure.
- Clock skew and failures outside the database transaction boundary require additional testing.
- At-most-once committed database effects do not guarantee exactly-once behavior for external services.

## Deployment verdict

**DON'T SHIP at 100 requests/second yet.**

The current tests provide useful evidence for idempotency, worker recovery, and protection against duplicate committed database effects. However, the required 10,000-job / 8-worker chaos test at the target load has not yet been demonstrated. Cancellation races, freeze scenarios, and broader failure conditions also need more coverage.

The first priority is to run the full target workload, preserve every raw result (including failures), and investigate any invariant violation before making a production-readiness claim.

## AI usage disclosure

AI assistance was used for code review, debugging, architecture discussion, and documentation. Suggestions were reviewed and tested against the implementation where possible. The final repository and test results should be treated as the source of truth; AI-generated suggestions were not accepted as proof of correctness without verification.
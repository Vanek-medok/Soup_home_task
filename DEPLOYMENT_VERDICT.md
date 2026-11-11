# Deployment Verdict

## Decision: DON'T SHIP

The service is not ready for production deployment at the target load of 100 requests per second.

The implementation has demonstrated several important correctness properties under limited chaos testing. In the latest recorded test, the service processed 100 jobs using 8 workers at 10 RPS while worker processes were repeatedly killed. The test completed successfully:

- 100 jobs reached the `succeeded` state.
- 100 database effects were recorded.
- 100 unique jobs had effects.
- No duplicate effects were detected.
- 12 jobs were reclaimed after worker failures.
- The maximum observed attempt count was 2.
- 14 worker kills were performed.
- 71 duplicate POST requests were generated.
- No submitted idempotency keys remained unacknowledged in the database.

These results provide evidence that job recovery, idempotent submission, and database-level fencing work under the tested conditions.

However, this test is not sufficient to justify production deployment at 100 RPS.

### Reasons for the decision

1. **Insufficient load testing.** The recorded successful chaos test used 100 jobs at 10 RPS, not the required 10,000 jobs at 100 RPS.

2. **Incomplete failure scenarios.** Cancellation races and worker freezing have not yet been validated in a sufficiently comprehensive target-load test.

3. **Insufficient performance data.** There is not enough evidence about API latency, throughput stability, database saturation, connection-pool behavior, and resource consumption at 100 RPS.

4. **Operational risks.** PostgreSQL is a central dependency. Database outages, connection exhaustion, and prolonged recovery periods require additional validation.

### Conditions for reconsidering the decision

Before deployment, I would require:

- A 10,000-job test with 8 workers and a target submission rate of 100 RPS.
- Repeated worker termination and freezing during processing.
- Concurrent duplicate submissions and cancellation requests.
- Verification that no jobs are silently lost or permanently stuck in `running`.
- Verification that no job produces more than one committed database effect.
- Measurements of API latency (p50, p95, p99), error rate, throughput, and database resource usage.
- Preservation of raw results for every test run, including failed runs.

### Final assessment

The current implementation demonstrates promising correctness under a limited failure scenario, but its behavior under the required production load has not been established.

**Decision: DON'T SHIP until the target-load and remaining chaos tests pass.**
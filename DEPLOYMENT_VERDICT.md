# Deployment Verdict

## Decision: DON'T SHIP (pending performance validation)

The service has demonstrated important correctness and recovery properties under the target workload of 100 requests per second, including concurrent submissions, duplicate requests, cancellation races, worker termination, and worker freezing.

The latest successful chaos test processed 10,000 jobs with worker failures and freezes. The test completed successfully with no reported invariant violations.

However, the available results do not include API latency percentiles or sufficient resource-utilization measurements. Therefore, the current evidence is not sufficient for an unconditional production deployment decision.

## Latest Chaos Test

Run ID: `20261111T182059Z`

Configuration:

- Jobs: 10,000
- Target submission rate: 100 RPS
- Concurrency: 128
- Duplicate submission rate: 30%
- Cancellation rate: 5 RPS
- Worker termination: SIGKILL every 2 seconds
- Worker freezing: enabled, 5% probability, 45 seconds
- Drain timeout: 20 minutes
- Post-test settling period: 30 seconds

Results:

- 10,000 jobs recorded in the database.
- 9,157 jobs reached `succeeded`.
- 843 jobs reached `cancelled`.
- 9,157 database effects were recorded.
- 113 worker kills were performed.
- 4 worker freezes and 4 corresponding thaws were performed.
- 109 jobs were reclaimed after worker failures.
- Maximum observed attempt count: 3.
- 7,486 duplicate POST requests were generated.
- All 10,000 idempotency keys were acknowledged.
- No unacknowledged idempotency keys remained in the database.
- No invariant violations were reported.
- Final harness result: `PASS`.

The machine-readable result and raw log are preserved in:

- `runs/20261111T182059Z/result.json`
- `runs/20261111T182059Z/run.log`

## Additional Target-Load Test

Run ID: `20261111T180339Z`

This test used the same 10,000-job workload and 100 RPS target, with repeated worker termination but without worker freezing.

Results:

- 10,000 jobs recorded.
- 9,193 succeeded.
- 807 cancelled.
- 116 worker kills.
- 106 jobs reclaimed.
- 9,193 database effects.
- No reported invariant violations.
- Final harness result: `PASS`.

Artifacts:

- `runs/20261111T180339Z/result.json`
- `runs/20261111T180339Z/run.log`

## Correctness and Recovery Assessment

The latest tests provide evidence for the following properties:

1. **Job recovery:** jobs abandoned by terminated or frozen workers can be reclaimed and processed again.
2. **Idempotent submission:** duplicate POST requests do not result in duplicate job creation.
3. **Fencing:** stale worker attempts do not produce reported invariant violations.
4. **Cancellation:** concurrent cancellation requests are exercised during processing.
5. **Eventual completion:** the harness drained the workload without jobs remaining queued or running.
6. **Effect consistency:** the number of recorded effects matches the number of successfully completed jobs in the reported results.

The harness reported `pass: true`, with empty `violation_counts` and no warnings in the latest run.

## Remaining Limitations

Despite successful correctness and chaos testing, several production-readiness questions remain open.

1. **API latency:** p50, p95, and p99 latency measurements were not collected. The current test results cannot establish API response-time guarantees.
2. **Resource utilization:** CPU, memory, database connection-pool saturation, and PostgreSQL resource consumption were not recorded as part of the final result.
3. **Database availability:** PostgreSQL outage and recovery scenarios require additional validation.
4. **Long-running stability:** the tests demonstrate recovery under the tested failure scenarios, but do not establish long-term stability under sustained production traffic.

## Conditions for Production Deployment

Before production deployment, I would require:

- API latency measurements (p50, p95, p99) under the target workload.
- HTTP error-rate and throughput measurements.
- CPU and memory monitoring for API and worker containers.
- PostgreSQL connection-pool and resource-utilization measurements.
- Validation of database outage and recovery behavior.

## Final Assessment

The implementation has passed the recorded 10,000-job chaos tests at a target submission rate of 100 RPS, including repeated worker termination and worker freezing.

These results provide substantially stronger evidence of correctness and recovery than the earlier 100-job test.

However, correctness under chaos testing and production performance are separate concerns. Since latency and resource-utilization measurements are unavailable, production readiness has not yet been fully established.

**Final Decision: DON'T SHIP until performance and operational validation are completed.**
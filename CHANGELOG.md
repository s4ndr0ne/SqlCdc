# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project follows
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

This release changes the defaults towards not losing data. Every one of them can be set back to
the previous behaviour explicitly; see **Migrating** below.

### Changed

- **The watermark is persisted in SQL Server by default.** A watcher built without `UseStateStore`
  now uses a `SqlCdcStateStore` on the watched database (table `dbo.cdc_watermark`, created on
  first use) instead of `InMemoryCdcStateStore`, so a restart resumes where it left off rather
  than skipping everything that happened in between under `FromNow`. The store shares the
  watcher's connection factory, so it authenticates the same way and saves on the poll connection.
  The account therefore needs `CREATE TABLE` rights on first use, or the table provisioned by
  `scripts/create-state-tables.sql` and `createTableIfMissing: false`.
- **`OnAcknowledgement` is the default checkpoint mode.** The watermark only moves past a batch
  once every change in it has been acknowledged. Handlers registered with `AddCdcChangeHandler`
  are acknowledged automatically; code that reads `watcher.Changes` directly must call
  `CdcChange.Acknowledge()` on every change, or polling stalls at the first batch (reported every
  30 seconds).
- **Leader election is on by default**, on a lease named after the watcher (`WithName`, `default`
  unless set). Replicas of one application share the name and elect a single active instance;
  watchers with different names never contend. `UseSingleActiveInstance` now takes an optional
  lease name (previously it defaulted to `SqlCdc`) to share a lease across differently named
  watchers, and the new `WithoutSingleActiveInstance` (`"SingleActiveInstance": false` in
  configuration) turns election off for a deployment with exactly one instance.
- **A dead-letter sink is required as soon as a handler is registered.** A handler that has used
  up its attempts has its change acknowledged, so without a sink the change was simply dropped.
  The host now fails to start with a message naming `AddCdcDeadLetterSink`.
- **A dead-letter sink that fails is retried** with the handler backoff (capped at one minute)
  until the write succeeds, instead of logging and dropping the dead letter. Delivery pauses
  meanwhile; under `OnAcknowledgement` the change also stays unacknowledged, so a restart replays
  it. The first failure is logged as an error, the retries as warnings.
- **Unsupported CDC rows fail the poll instead of being skipped.** A row with an `__$operation`
  value outside 1–4, or an update image without its counterpart, now throws: the poll of that
  capture instance fails and is retried with backoff, the watermark does not move past the row,
  `sqlcdc.poll.failures` counts it and the health check turns unhealthy. The other capture
  instances keep polling. `SqlCdcDiagnostics.SkippedRowsMetric` is obsolete and no longer emitted.
- **The token passed to `StartAsync` no longer stops the watcher.** It bounds the startup work
  only; the polling loop runs until `StopAsync` or `DisposeAsync`. A caller's startup timeout can
  therefore no longer stop a healthy watcher minutes later.
- The change function is read with `TOP (BatchSize + 1)` ordered by `__$start_lsn`, the leading
  column of the change table's clustered index, so each poll streams one batch in index order.
  Update images are paired whichever comes first, so the order inside a transaction is not
  relied upon.
- While delivery is stalled on a slow or dead consumer for more than 30 seconds, the poll
  connection is given back to the pool and taken again for the checkpoint, so a stall that lasts
  hours does not pin a connection. The usual batch never pays the round-trip.
- The lease is verified between tables and between batches as well as at the top of the polling
  cycle (still throttled by `WithLeaseKeepaliveInterval`), so a wait on a slow consumer that
  outlasts the interval is followed by a real check before the next batch is read, rather than by
  more batches from an instance that may no longer be the leader.
- The lease provider clears the connection pool whenever it drops its connection, so a custom
  connection factory handing out pooled connections cannot leave the session lock in an idle pool.
- Polling a capture instance now uses a single connection for the whole cycle — log bounds,
  changes and commit-time mapping — instead of opening one per round-trip.
- The delay after a polling error now doubles with each consecutive failure, capped at 5 minutes,
  instead of staying fixed at `WithRetryDelay`. The configured value is the initial delay, and a
  successful poll resets the backoff.

### Migrating

- No persistent watermark wanted (tests, deliberately ephemeral consumers):
  `UseStateStore(new InMemoryCdcStateStore())`.
- No DDL rights at runtime: run `scripts/create-state-tables.sql` and pass
  `UseStateStore(new SqlCdcStateStore(connectionString, createTableIfMissing: false))`.
- Previous checkpoint behaviour: `WithCheckpointMode(CdcCheckpointMode.OnEmit)`.
- Exactly one instance and no lease connection wanted: `WithoutSingleActiveInstance()`.
- Several watchers that previously shared the implicit `SqlCdc` lease: `UseSingleActiveInstance("SqlCdc")`.
- Handlers without a sink: `AddCdcDeadLetterSink(new SqlCdcDeadLetterSink(connectionString))`.

### Added

- `CdcWatcherStatus.ConsecutiveLeaseFailures` and `StandbySince`, the `sqlcdc.lease.failures`
  counter, and `SqlCdcHealthCheckOptions.MaxStandbyDuration`. A standby whose lease attempts throw
  is now reported degraded, then unhealthy after `UnhealthyAfterConsecutiveFailures`, instead of
  hiding behind the "standing by is healthy" rule; a standby that lasts longer than
  `MaxStandbyDuration` (opt-in) is reported degraded.
- `WithLeaseKeepaliveInterval` (`LeaseKeepaliveInterval` in configuration): how often the active
  instance verifies it still holds the lease. Defaults to 10 seconds; previously the lease was
  checked on every polling cycle, which with a short poll interval dominated the traffic on the
  lease connection.
- `WithoutSingleActiveInstance` on the builder, and `false` for `SingleActiveInstance` in
  configuration, to run without leader election.

### Fixed

- Catching up on a large backlog cost time quadratic in its size: the batch cap stopped reading
  after `WithBatchSize` rows, but the query still returned the whole remaining range and the
  client drained it on every poll. Each poll now transfers one batch (see Changed). A single
  transaction larger than the batch size is still read in full, in a second query bounded to its
  own LSN.
- With a `SqlCdcStateStore` pointed at a different database (or credentials) than the watched
  one, watermarks were read through the store's connection but written on the poll connection —
  into the wrong database — and never found again after a restart. The poll connection is now
  reused only when the store targets the same connection string and token callback, or the very
  same connection factory instance; otherwise the store opens its own connection per save.
- A watcher that acquired the lease but could not load its watermarks — the state store being
  unavailable, for instance — kept the lock and blocked every standby. It now releases the lease
  and retries from the standby position, so another instance can take over.
- The watermark and dead-letter tables are created idempotently on first use. Concurrent first
  use by several replicas previously collided on `CREATE TABLE` with error 2714 ("there is already
  an object named…"), which self-healed on retry but logged a spurious error. The 2714 race is now
  treated as a missing-object retry, the same as the 208 it already handled.
- A polling loop that terminated on an unexpected error kept the lease, so no standby could take
  over until the process exited. The lease is now released whenever the loop ends, whatever the
  reason.
- After such a crash the hosted service ended silently and the host kept running with CDC dead.
  It now restarts the watcher (after `WithRetryDelay`, with the usual backoff on repeated start
  failures), whether or not handlers are registered; a deliberate `StopAsync` by the application
  still leaves the watcher stopped.
- A table with no changes for longer than the CDC retention period (3 days by default) triggered
  the "changes … were removed by the CDC cleanup job and are lost" warning even though no change
  ever existed: its watermark never moved, so the cleanup job eventually trimmed past it. An empty
  poll now records "read up to here, nothing found" by advancing the watermark to the current max
  LSN, persisted at most once every 5 minutes per table so an idle table does not cost a write per
  poll. The warning now only fires when changes could actually have been lost — a watcher that was
  stopped for longer than the retention period.
- A dead consumer with a full channel parked the poller inside the channel write with nothing in
  the logs — the acknowledgement warning never fired because the batch was still being published.
  A blocked write is now reported every 30 seconds with the queue length.
- `sys.fn_cdc_get_max_lsn()` returning NULL (the CDC capture job never processed a transaction)
  was silently indistinguishable from an idle database: the poll "succeeded" and the watcher
  looked healthy while nothing could ever be delivered. It is now reported with a warning, once
  per occurrence.
- Setting `SingleActiveInstance`/`LeaseName` in the configuration section silently discarded an
  `ICdcLeaseProvider` registered in the container. The contradiction now fails at startup with
  both sides named.
- If `sp_releaseapplock` fails on a pooled lease connection (custom connection factories may hand
  those out), the connection went back to the pool still holding the session lock, delaying
  failover until the pool reused it. The pool is now cleared so disposing truly ends the session.
- The static metrics registry pinned undisposed watchers forever (and kept reporting their
  gauges). It now holds them weakly: a collected watcher drops out of the metrics on its own.
- Concurrent first use across processes could fail transiently on `CREATE TABLE` (error 2714)
  inside the ensure step itself; it is now treated as "the table exists", which is all the ensure
  has to guarantee.
- `SaveLastLsnAsync` left `SET XACT_ABORT ON` on the shared polling connection for the rest of
  the poll; the batch now restores it.
- `GetStatus()` read the per-table timestamps without synchronization, allowing torn reads from
  health probes; they are now stored as atomically-read ticks.

## [2.0.0] - 2026-08-14

A major release. Two of the breaking changes below are binary but not source breaking, so the
upgrade is a recompile rather than an edit — with the exception of the dependency floors, which a
consumer pinned to `Microsoft.Data.SqlClient` 6.x has to move as well.

### Breaking

- `SqlCdcWatcher.StopAsync()` now takes an optional `CancellationToken`. Source compatible, but
  an assembly compiled against 1.0.0 calls the parameterless overload and will fail with
  `MissingMethodException` until it is recompiled.
- `SqlCdcStateStore(string, string, string)` gained optional `createTableIfMissing` and
  `commandTimeout` parameters, with the same consequence.
- `CdcWatcherOptions.ConnectionString` is no longer `required` and is now nullable: a connection
  factory can take its place.
- Watermarks are now written **monotonically**. `ICdcStateStore.SaveLastLsnAsync` with an LSN
  lower than the stored one is a no-op instead of a rewind, in both the SQL and in-memory stores.
  Rewinding a watermark on purpose now means editing the table.
- When a table has two capture instances and none is specified, the **oldest** is used rather
  than whichever the server returned first. Previously the choice was undefined.
- The dependency floors moved up by a major: `Microsoft.Data.SqlClient` from 6.0.1 to 7.0.2, and
  the `Microsoft.Extensions.*` packages from 9.0.0 to 10.0.0 because SqlClient 7 requires them.
  `SqlConnection`, `SqlAuthenticationParameters` and `SqlAuthenticationToken` are part of the
  public API through `ICdcConnectionFactory` and `UseAccessTokenCallback`, so a consumer pinned to
  SqlClient 6.x cannot restore this package without upgrading too. `net8.0` is still supported.

### Added

- **Single active instance.** `UseSingleActiveInstance()` elects one polling watcher across
  replicas through a SQL Server session-scoped application lock (`sp_getapplock`). A standby
  reloads its watermarks on taking over. Custom election through `ICdcLeaseProvider`.
- **At-least-once end to end.** `WithCheckpointMode(CdcCheckpointMode.OnAcknowledgement)` holds
  the watermark until every change in the batch has been acknowledged; registered handlers
  acknowledge automatically, direct consumers call `CdcChange.Acknowledge()`.
- **Handler retry and dead-lettering.** `WithHandlerRetry(maxAttempts, retryDelay)` with
  exponential backoff, and `ICdcDeadLetterSink` / `SqlCdcDeadLetterSink` for changes that use up
  their attempts.
- **Metrics and tracing.** A `Meter` and an `ActivitySource` named `SqlCdc`
  (`SqlCdcDiagnostics.MeterName` / `.ActivitySourceName`) covering throughput, end-to-end lag,
  poll duration and failures, channel backlog, leadership, handler duration and dead letters.
- **Health check.** `AddHealthChecks().AddSqlCdc()`, plus `SqlCdcWatcher.GetStatus()` for the
  same snapshot in code. A standby instance reports healthy.
- **Configuration binding.** `AddSqlCdc(IConfiguration)` binds the `SqlCdc` section
  (`SqlCdcConfiguration`), with code-configured settings taking precedence.
- **Connection factory.** `ICdcConnectionFactory` is used for every connection — CDC reads,
  watermarks, dead letters and the lease — via `UseConnectionFactory(...)`, with
  `UseAccessTokenCallback(...)` for application-driven Entra ID tokens.
- `WithName(...)` to tell several watchers apart in metrics and health data.
- `createTableIfMissing` and `commandTimeout` on `SqlCdcStateStore` and `SqlCdcDeadLetterSink`,
  with [`scripts/create-state-tables.sql`](scripts/create-state-tables.sql) for environments
  without DDL rights at runtime.

### Fixed

- A table with two capture instances (the normal state during a schema migration) no longer
  resolves to an arbitrary one, and the choice is logged.
- An explicit capture instance that is not defined for the table now fails at startup listing the
  ones that are, instead of failing every poll with `Invalid object name`.
- The watermark upsert is atomic: concurrent writers could previously both find the row missing
  and collide on the primary key.
- End-to-end lag is measured against the SQL Server clock rather than the client's UTC, so it is
  correct when the server is in another time zone.

### Changed

- The package now depends on `Microsoft.Extensions.Configuration.Binder` and
  `Microsoft.Extensions.Diagnostics.HealthChecks`.
- Builds are deterministic and SourceLink-enabled, so the published symbols resolve to sources.
- Package validation runs against the previously published version.

## [1.0.0]

First release.

[2.0.0]: https://github.com/s4ndr0ne/SqlCdc/compare/v1.0.0...v2.0.0
[1.0.0]: https://github.com/s4ndr0ne/SqlCdc/releases/tag/v1.0.0

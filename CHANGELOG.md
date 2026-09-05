# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project follows
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `WithLeaseKeepaliveInterval` (`LeaseKeepaliveInterval` in configuration): how often the active
  instance verifies it still holds the lease. Defaults to 10 seconds; previously the lease was
  checked on every polling cycle, which with a short poll interval dominated the traffic on the
  lease connection.
- `sqlcdc.skipped.rows`: counts CDC rows whose `__$operation` value is not supported, which are
  skipped rather than delivered — see Delivery semantics.

### Changed

- Polling a capture instance now uses a single connection for the whole cycle — log bounds,
  changes and commit-time mapping — instead of opening one per round-trip.
- The delay after a polling error now doubles with each consecutive failure, capped at 5 minutes,
  instead of staying fixed at `WithRetryDelay`. The configured value is the initial delay, and a
  successful poll resets the backoff.

### Fixed

- Catching up on a large backlog cost time quadratic in its size: the batch cap stopped reading
  after `WithBatchSize` rows, but the query still returned the whole remaining range and the
  client drained it on every poll. The change function is now queried with `TOP (BatchSize + 1)`
  in the change table's clustered order, so each poll transfers one batch. A single transaction
  larger than the batch size is still read in full, in a second query bounded to its own LSN.
- With a `SqlCdcStateStore` pointed at a different database (or credentials) than the watched
  one, watermarks were read through the store's connection but written on the poll connection —
  into the wrong database — and never found again after a restart. The poll connection is now
  reused only when the store targets the same connection string and token callback, or the very
  same connection factory instance; otherwise the store opens its own connection per save.
- CDC rows with an unsupported `__$operation` value were dropped silently while the watermark
  still advanced past them. They are now counted in `sqlcdc.skipped.rows` and reported with a
  warning (one per operation value), so the loss is visible in logs and metrics.
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
  failures); a deliberate `StopAsync` by the application still leaves the watcher stopped.
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
  failover until the pool reused it. The pool is now cleared so disposing truly ends the session,
  and a custom factory is reminded at construction that the lease connection should be unpooled.
- The static metrics registry pinned undisposed watchers forever (and kept reporting their
  gauges). It now holds them weakly: a collected watcher drops out of the metrics on its own.
- An update before-image (`__$operation` = 3) without its after-image was dropped silently; it is
  now counted in `sqlcdc.skipped.rows` and reported with a warning, like unknown operations.
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

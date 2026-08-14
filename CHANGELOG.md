# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project follows
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

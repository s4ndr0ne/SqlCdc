# SqlCdc

Real-time Change Data Capture (CDC) for **SQL Server**, built on top of native CDC.
The package polls capture instances (`cdc.fn_cdc_get_all_changes_*`), reconstructs
events (insert/update/delete with *before*/*after* images) and delivers them over a
`System.Threading.Channels.Channel<CdcChange>`.

## Features

- Native SQL Server CDC: no extra columns added to source tables.
- Rich events: **before/after** image, operation type, LSN, commit time, and **update mask**.
- Persistent LSN watermark: the watcher resumes exactly where it left off.
- Bounded channel with backpressure: a slow consumer stalls the poller instead of losing events.
- Fluent API (`SqlCdcWatcherBuilder`).

## Prerequisites

- SQL Server (2016+) with CDC enabled on the database and the target tables:

```sql
-- Requires sysadmin or db_owner
EXEC sys.sp_cdc_enable_db;

EXEC sys.sp_cdc_enable_table
     @source_schema = N'dbo',
     @source_name   = N'Orders',
     @role_name     = NULL;
```

## Usage

```csharp
using SqlCdc;

var watcher = SqlCdcWatcherBuilder
    .Create()
    .UseConnectionString("Server=.;Database=MyDb;TrustServerCertificate=True")
    .WatchTable("dbo", "Orders")
    .WatchTable("dbo", "Customers")
    .WithPollInterval(TimeSpan.FromMilliseconds(250))
    .UseStateStore(new SqlCdcStateStore(connectionString))  // resume after restart
    .Build();

await watcher.StartAsync(cts.Token);

await foreach (var change in watcher.Changes.WithCancellation(cts.Token))
{
    Console.WriteLine($"{change.TableName} {change.Operation}");
    foreach (var (column, value) in change.After)
        Console.WriteLine($"  {column} = {value}");
}
```

### ASP.NET Core / Generic Host

```csharp
builder.Services.AddSqlCdc(cdc => cdc
    .UseConnectionString(connectionString)
    .WatchTable("dbo", "Orders")
    .UseStateStore(new SqlCdcStateStore(connectionString)));

builder.Services.AddCdcChangeHandler<OrderChangedHandler>();
```

`AddSqlCdc` registers the watcher as a singleton and an `IHostedService` that starts it
with the host and stops it on shutdown. The logger and `ICdcStateStore` are resolved from
the container when available; settings configured inside the delegate take precedence.

```csharp
public sealed class OrderChangedHandler(AppDbContext db) : ICdcChangeHandler
{
    public async Task HandleAsync(CdcChange change, CancellationToken ct = default)
    {
        // ...
    }
}
```

Each handler is **scoped** and resolved in a dedicated scope per event, so injecting a
`DbContext` is safe. All registered handlers receive every event in registration order.
If a handler throws, the error is logged and the event is **dropped**: the watermark
advances when the event enters the channel, not when it is handled, so retry is the
handler's responsibility.

When no handlers are registered the watcher is still started by the host. Inject
`SqlCdcWatcher` and consume `Changes` directly.

### Event model

```csharp
record CdcChange
{
    string CaptureInstance;
    string SourceSchema, SourceTable;
    CdcOperationType Operation;        // Insert | Update | Delete
    byte[] StartLsn, SeqVal;
    DateTime CommitTime;
    IReadOnlyDictionary<string, object?> Before;
    IReadOnlyDictionary<string, object?> After;
    IReadOnlyDictionary<string, bool> UpdateMask;  // changed columns (update only)
    string Key;                                     // stable per-change identifier
}
```

## Options

| Builder method | Default | Description |
|---|---|---|
| `WithPollInterval` | 500 ms | Polling frequency per capture instance |
| `WithBatchSize` | 1000 | Rows per cycle per table (soft cap, see below) |
| `WithChannelCapacity` | 100 000 | Channel capacity (backpressure) |
| `StartFrom` | `FromNow` | `FromNow` (skip history) or `FromBeginning` |
| `UseStateStore` | in-memory | `SqlCdcStateStore` to persist the watermark LSN |
| `WithRetryDelay` | 5 s | Delay after a polling error |

## Delivery semantics

Batches are always cut on a transaction (LSN) boundary so the *before* and *after* images
of an update stay together. For this reason `WithBatchSize` is a **soft** cap: a single
transaction larger than the batch size is read in full (with a warning logged).

The channel is **bounded**: if the consumer falls behind, the poller blocks
(`BoundedChannelFullMode.Wait`). The LSN watermark is saved after each completed batch,
so events are delivered **at-least-once**: after a crash a batch may be re-emitted on
restart. Consumers should deduplicate using `CdcChange.Key` if needed.

### CDC retention

SQL Server's cleanup job trims change tables beyond the configured retention window
(3 days by default). If the service is down longer than that, the saved watermark points
to rows that no longer exist: the watcher **resumes from the oldest available LSN** and
logs a warning. Changes in between are lost — a data loss that is explicit rather than
a silent error loop. For longer outage windows, increase the retention:
`EXEC sys.sp_cdc_change_job @job_type='cleanup', @retention=<minutes>`.

### Error isolation

Each table has independent error state: if one capture instance fails (permissions, CDC
disabled, table dropped), the others continue polling normally. The failing instance is
retried after `WithRetryDelay`, not on every polling cycle.

## Development setup

```bash
dotnet restore
dotnet build SqlCdc.slnx
dotnet test tests/SqlCdc.Tests                    # unit tests, no external dependencies
dotnet test tests/SqlCdc.IntegrationTests         # requires Docker (see below)
dotnet pack src/SqlCdc/SqlCdc.csproj -c Release   # build NuGet package
```

### Integration tests

Run against a real SQL Server instance started with [Testcontainers](https://dotnet.testcontainers.org/).
**Docker** must be running. The container starts with SQL Server Agent enabled because without
it the CDC capture job does not run and the change tables stay empty. On Apple Silicon the image
is amd64 and runs under emulation (Rosetta must be enabled in Docker Desktop).

A full run takes roughly 30 seconds plus initial container startup. To skip them:

```bash
dotnet test SqlCdc.slnx --filter "Category!=Integration"
```

## Sample

```bash
SQLCDC_CONNECTION="Server=.;Database=MyDb;User Id=sa;Password=...;TrustServerCertificate=True" \
  dotnet run --project samples/SqlCdc.Sample
```

See `scripts/enable-cdc.sql` to enable CDC on a sample table.


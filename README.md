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
- **Single active instance**: leader election over a SQL application lock, so a multi-replica
  deployment does not emit every change N times.
- **At-least-once end to end**: an opt-in checkpoint mode that advances the watermark only once
  changes have actually been processed.
- **Observable**: `Meter` and `ActivitySource` for OpenTelemetry, plus a health check that knows
  the difference between "idle" and "stuck".
- **Handler retry and dead-letter queue**: a failing change is retried and then parked for
  inspection instead of vanishing into a log line.
- **Configurable from `IConfiguration`**, with Entra ID / Managed Identity and a connection
  factory for anything the connection string cannot express.
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
A handler that throws is retried according to `WithHandlerRetry` and then dead-lettered or
dropped — see [Handler failures](#handler-failures-retry-and-dead-letter-queue).

When no handlers are registered the watcher is still started by the host. Inject
`SqlCdcWatcher` and consume `Changes` directly.

### Configuration

Everything can come from a configuration section instead of code:

```csharp
builder.Services.AddSqlCdc(builder.Configuration.GetSection("SqlCdc"));
```

```jsonc
{
  "SqlCdc": {
    "ConnectionString": "Server=.;Database=Sales;Authentication=Active Directory Default;Encrypt=True",
    "Name": "sales",
    "Tables": [
      { "Schema": "dbo", "Table": "Orders" },
      { "Schema": "dbo", "Table": "Customers", "CaptureInstance": "dbo_Customers_v2" }
    ],
    "PollInterval": "00:00:00.500",
    "BatchSize": 1000,
    "ChannelCapacity": 100000,
    "StartMode": "FromNow",              // FromNow | FromBeginning
    "CheckpointMode": "OnAcknowledgement", // OnEmit | OnAcknowledgement
    "RetryDelay": "00:00:05",
    "CommandTimeout": "00:00:30",
    "LeaseName": "sales",                 // setting it turns on single-active-instance
    "LeaseRetryDelay": "00:00:10",
    "MaxHandlerAttempts": 3,
    "HandlerRetryDelay": "00:00:01"
  }
}
```

`Schema` defaults to `dbo`. Any setting left out keeps its default, so a section can be as small
as a connection string and a table. Mixing is fine — the section is applied first and the
optional delegate second, so code wins:

```csharp
builder.Services.AddSqlCdc(
    builder.Configuration.GetSection("SqlCdc"),
    cdc => cdc.UseStateStore(new SqlCdcStateStore(connectionString)));
```

Configuration errors surface when the host starts, not at the first poll: a section that does
not exist, a table without a name, or a missing connection string all fail `StartAsync`.

### Connections and authentication

`Authentication=Active Directory Default` (or `... Managed Identity`, `... Workload Identity`)
in the connection string is handled by Microsoft.Data.SqlClient and needs nothing from this
package.

When the application wants to own how connections are made — a configured `TokenCredential`, a
`SqlRetryLogicBaseProvider` for Azure SQL's transient faults, a custom `SqlConnectionStringBuilder`
per tenant — supply a factory. Everything then goes through it: CDC reads, the watermark table,
the dead-letter table and the lease.

```csharp
builder.Services.AddSqlCdc(cdc => cdc
    .UseConnectionFactory(async ct =>
    {
        var connection = new SqlConnection(connectionString)
        {
            RetryLogicProvider = SqlConfigurableRetryFactory.CreateExponentialRetryProvider(retryOptions),
        };
        await connection.OpenAsync(ct);
        return connection;
    })
    .WatchTable("dbo", "Orders"));
```

The factory must hand out a **new** connection per call — they are opened and disposed per
operation. Returning a connection that is not open yet is fine; it gets opened for you.

To own only token acquisition, keep the connection string and add a callback:

```csharp
var credential = new DefaultAzureCredential();

builder.Services.AddSqlCdc(cdc => cdc
    .UseConnectionString(connectionString)
    .UseAccessTokenCallback(async (parameters, ct) =>
    {
        var token = await credential.GetTokenAsync(
            new TokenRequestContext(["https://database.windows.net/.default"]), ct);
        return new SqlAuthenticationToken(token.Token, token.ExpiresOn);
    })
    .WatchTable("dbo", "Orders"));
```

Registering an `ICdcConnectionFactory` in DI is picked up by `AddSqlCdc` automatically, and
`SqlCdcStateStore`, `SqlCdcDeadLetterSink` and `SqlApplicationLockLeaseProvider` all accept one
in their constructor.

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
| `WithCommandTimeout` | 30 s | Timeout for each SQL round-trip against the CDC database |
| `WithCheckpointMode` | `OnEmit` | When the watermark is persisted (see below) |
| `UseSingleActiveInstance` | off | Elect one active watcher across replicas |
| `WithLeaseRetryDelay` | 10 s | How often a standby instance retries the lease |
| `WithHandlerRetry` | 1 attempt | Attempts per handler before a change is dead-lettered |
| `WithName` | `default` | Name of the watcher in metrics and health data |
| `UseConnectionFactory` | connection string | Opens every connection through your own factory |
| `UseAccessTokenCallback` | none | Acquires an Entra ID token per connection |

## Delivery semantics

Batches are always cut on a transaction (LSN) boundary so the *before* and *after* images
of an update stay together. For this reason `WithBatchSize` is a **soft** cap: a single
transaction larger than the batch size is read in full (with a warning logged).

The channel is **bounded**: if the consumer falls behind, the poller blocks
(`BoundedChannelFullMode.Wait`). Consumers should deduplicate using `CdcChange.Key`: in both
checkpoint modes a batch can be re-emitted after a restart.

### Checkpoint mode

`WithCheckpointMode` decides *when* the watermark LSN is persisted, which is what a restart
resumes from.

| Mode | Watermark advances | After a crash |
|---|---|---|
| `OnEmit` (default) | when the batch is written to the channel | changes still in the channel, or being handled, are **not** redelivered |
| `OnAcknowledgement` | when every change of the batch has been acknowledged | the whole unacknowledged batch is redelivered |

`OnEmit` is the cheapest and fine when losing an in-flight change is acceptable. Choose
`OnAcknowledgement` for at-least-once delivery end to end:

```csharp
builder.Services.AddSqlCdc(cdc => cdc
    .UseConnectionString(connectionString)
    .WatchTable("dbo", "Orders")
    .UseStateStore(new SqlCdcStateStore(connectionString))
    .WithCheckpointMode(CdcCheckpointMode.OnAcknowledgement));
```

Registered `ICdcChangeHandler`s are acknowledged automatically, once every handler has been
given the change. Consuming `watcher.Changes` directly means acknowledging by hand:

```csharp
await foreach (var change in watcher.Changes.WithCancellation(ct))
{
    await ProcessAsync(change);
    change.Acknowledge();   // required: without it the watermark never advances
}
```

Polling pauses at the batch boundary until the batch is acknowledged, so throughput follows
the consumer. A change that is never acknowledged stalls polling for that table, and the stall
is logged every 30 seconds rather than left silent.

Note that acknowledgement covers *crashes*, not handler bugs: a handler that throws still has
its change acknowledged and dropped (with an error logged), so one bad event cannot block the
pipeline. Retrying is the handler's responsibility.

### Running more than one instance

By default the watcher assumes it is the only one: with several replicas, each would poll the
same capture instances, deliver the same changes and overwrite the others' watermark.
`UseSingleActiveInstance` elects one active watcher:

```csharp
builder.Services.AddSqlCdc(cdc => cdc
    .UseConnectionString(connectionString)
    .WatchTable("dbo", "Orders")
    .UseStateStore(new SqlCdcStateStore(connectionString))   // must be shared between instances
    .UseSingleActiveInstance());
```

The lease is a session-scoped SQL Server application lock (`sp_getapplock`) held on a dedicated
connection to the watched database. SQL Server drops it as soon as that connection goes away, so
a crashed, killed or partitioned instance loses the lease on its own: there is no expiry to tune
and no clock to keep in sync, and two instances can never poll at the same time.

Standby instances retry every `WithLeaseRetryDelay` and expose `SqlCdcWatcher.IsLeader`. On
taking over, a standby reloads the watermarks from the state store — which therefore has to be
a shared one (`SqlCdcStateStore`, not the in-memory default) — and continues from where the
previous leader had checkpointed. A graceful shutdown releases the lease, so failover is
immediate rather than waiting for SQL Server to notice a dead session.

Give the lease a name to run independent watchers (different table sets) against the same
database: `UseSingleActiveInstance("orders")`. For a different election mechanism altogether,
implement `ICdcLeaseProvider` and pass it with `UseLeaseProvider` (or register it in DI, where
`AddSqlCdc` picks it up automatically).

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

## Handler failures, retry and dead-letter queue

By default a handler is called **once** per change: if it throws, the error is logged and the
change is dropped. That is one transient timeout away from losing an event, so configure both
a retry and somewhere for the leftovers to go:

```csharp
builder.Services.AddCdcChangeHandler<OrderChangedHandler>();
builder.Services.AddCdcDeadLetterSink(new SqlCdcDeadLetterSink(connectionString));

builder.Services.AddSqlCdc(cdc => cdc
    .UseConnectionString(connectionString)
    .WatchTable("dbo", "Orders")
    .WithHandlerRetry(maxAttempts: 3, retryDelay: TimeSpan.FromSeconds(1)));
```

The delay doubles with each attempt (1 s, 2 s, 4 s …) up to one minute. When the attempts run
out the change goes to the `ICdcDeadLetterSink`, and delivery continues: one poisonous change
must never block everything queued behind it.

`SqlCdcDeadLetterSink` writes to `dbo.cdc_dead_letter` (created automatically), keeping the
before/after images as JSON alongside the handler name, the attempt count and the last
exception — enough to inspect and replay. Implement `ICdcDeadLetterSink` to send them anywhere
else. A sink that throws is logged and the change is dropped: an unavailable sink slows nothing
down, but it does mean the dead letter itself is lost, so keep the write cheap.

Retries and dead-lettering apply to handlers registered with `AddCdcChangeHandler`. Code that
reads `watcher.Changes` directly owns its own error handling.

## Observability

The package publishes a `Meter` and an `ActivitySource`, both named `SqlCdc`:

```csharp
builder.Services.AddOpenTelemetry()
    .WithMetrics(m => m.AddMeter(SqlCdcDiagnostics.MeterName))
    .WithTracing(t => t.AddSource(SqlCdcDiagnostics.ActivitySourceName));
```

| Metric | Kind | What it tells you |
|---|---|---|
| `sqlcdc.changes.emitted` | counter | Throughput, by capture instance and operation |
| `sqlcdc.change.lag` | histogram (s) | **Freshness**: how old a change was when it was emitted |
| `sqlcdc.poll.duration` | histogram (s) | Time spent polling one capture instance |
| `sqlcdc.poll.failures` | counter | Failing polls — rising here with a flat emitted count is the alert |
| `sqlcdc.batch.rows` | histogram | Rows per batch; at the batch size, the poller is the bottleneck |
| `sqlcdc.channel.length` | gauge | Consumer backlog |
| `sqlcdc.leader` | gauge | 1 on the active instance, 0 on a standby |
| `sqlcdc.handler.duration` | histogram (s) | Handler time, by handler and outcome |
| `sqlcdc.handler.failures` | counter | Handler attempts that threw, retries included |
| `sqlcdc.dead_letters` | counter | Changes that used up their attempts |

Lag is measured entirely against the SQL Server clock — `fn_cdc_map_lsn_to_time` returns the
server's *local* time, so comparing it with the client's UTC would measure the time zone
offset. It costs no extra round-trip.

Spans: `SqlCdc.Poll` per poll of a capture instance, `SqlCdc.Handle` per handler call (tagged
with the change key, operation and attempt number).

### Health check

```csharp
builder.Services.AddHealthChecks().AddSqlCdc(tags: ["ready"]);
```

| Reported | When |
|---|---|
| Unhealthy | The watcher is not running, or a capture instance has failed `UnhealthyAfterConsecutiveFailures` polls in a row (3 by default) |
| Degraded | Some recent polling failures, or — if `MaxTimeSinceLastPoll` is set — no successful poll within it |
| Healthy | Polling normally, **or standing by** while another instance holds the lease |

A standby is deliberately healthy: it is doing exactly what it should, and failing its probe
would take a good replica out of rotation. The check reports per capture instance failure
counts, changes emitted and time since the last successful poll in its `data`, and the same
snapshot is available programmatically from `SqlCdcWatcher.GetStatus()`.

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


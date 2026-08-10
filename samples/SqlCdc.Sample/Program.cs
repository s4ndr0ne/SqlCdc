using SqlCdc;

var connectionString = Environment.GetEnvironmentVariable("SQLCDC_CONNECTION")
    ?? "Server=localhost;Database=MyDb;User Id=sa;Password=Your_password123;TrustServerCertificate=True";

var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    cts.Cancel();
};

var watcher = SqlCdcWatcherBuilder
    .Create()
    .UseConnectionString(connectionString)
    .WatchTable("dbo", "Orders")
    .WatchTable("dbo", "Customers")
    .WithPollInterval(TimeSpan.FromMilliseconds(250))
    .UseStateStore(new SqlCdcStateStore(connectionString))
    .Build();

await watcher.StartAsync(cts.Token);
Console.WriteLine($"Watching {watcher.Channel.Reader.CanCount} tables. Press Ctrl+C to stop.");

await foreach (var change in watcher.Changes.WithCancellation(cts.Token))
{
    Console.WriteLine($"[{change.CommitTime:O}] {change.TableName} {change.Operation}");
    foreach (var (column, value) in change.After)
    {
        if (change.UpdateMask.TryGetValue(column, out var updated) && updated)
        {
            Console.WriteLine($"    ~ {column} = {value ?? "NULL"}");
        }
        else
        {
            Console.WriteLine($"      {column} = {value ?? "NULL"}");
        }
    }
}

Console.WriteLine("Stopped.");

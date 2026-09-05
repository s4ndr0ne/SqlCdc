using SqlCdc;

var connectionString = Environment.GetEnvironmentVariable("SQLCDC_CONNECTION")
    ?? "Server=localhost;Database=MyDb;User Id=sa;Password=Your_password123;TrustServerCertificate=True";

var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    cts.Cancel();
};

// Disposing stops the poll, completes the channel and releases the lease. The token passed to
// StartAsync only bounds the startup work, so it is the dispose that stops the watcher on Ctrl+C.
await using var watcher = SqlCdcWatcherBuilder
    .Create()
    .UseConnectionString(connectionString)
    .WatchTable("dbo", "Orders")
    .WatchTable("dbo", "Customers")
    .WithPollInterval(TimeSpan.FromMilliseconds(250))
    .Build();

await watcher.StartAsync(cts.Token);
Console.WriteLine("Watching dbo.Orders and dbo.Customers. Press Ctrl+C to stop.");

try
{
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

        // The default checkpoint mode is OnAcknowledgement: the watermark only moves past a batch
        // once every change in it has been acknowledged.
        change.Acknowledge();
    }
}
catch (OperationCanceledException) when (cts.IsCancellationRequested)
{
    // Ctrl+C.
}

await watcher.StopAsync();
Console.WriteLine("Stopped.");

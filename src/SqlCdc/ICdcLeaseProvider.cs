namespace SqlCdc;

/// <summary>
/// Elects a single active watcher when several instances of the application run against the same
/// database. Without one, every instance polls the same capture instances, emits the same events
/// and overwrites the others' watermark.
/// </summary>
/// <remarks>
/// The watcher polls only while it holds the lease. It calls <see cref="IsHeldAsync"/> every
/// <see cref="CdcWatcherOptions.LeaseKeepaliveInterval"/> (10 seconds by default) and keeps
/// polling in between on the assumption the lease is still held.
/// </remarks>
public interface ICdcLeaseProvider : IAsyncDisposable
{
    /// <summary>
    /// Tries to take the lease without waiting. Returns <c>true</c> when this instance may poll,
    /// <c>false</c> when another instance holds it.
    /// </summary>
    Task<bool> TryAcquireAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Confirms the lease is still held. Returning <c>false</c> pauses polling: the watcher reloads
    /// its watermarks from the state store before it resumes, since another instance may have
    /// advanced them in the meantime.
    /// </summary>
    Task<bool> IsHeldAsync(CancellationToken cancellationToken = default);

    /// <summary>Gives up the lease so another instance can take over without waiting for a timeout.</summary>
    Task ReleaseAsync(CancellationToken cancellationToken = default);
}

/// <summary>
/// Lease provider for a single-instance deployment: the lease is always held. This is the default,
/// and it is why running two watchers against the same database needs an explicit opt-in.
/// </summary>
internal sealed class NullCdcLeaseProvider : ICdcLeaseProvider
{
    public static readonly NullCdcLeaseProvider Instance = new();

    public Task<bool> TryAcquireAsync(CancellationToken cancellationToken = default) => Task.FromResult(true);

    public Task<bool> IsHeldAsync(CancellationToken cancellationToken = default) => Task.FromResult(true);

    public Task ReleaseAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}

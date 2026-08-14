using System.Collections.Concurrent;

namespace SqlCdc;

/// <summary>
/// In-memory state store. Watermarks are lost when the process exits.
/// Suitable for tests and short-lived processes.
/// </summary>
public sealed class InMemoryCdcStateStore : ICdcStateStore
{
    private readonly ConcurrentDictionary<string, byte[]> _watermarks = new();

    public Task<byte[]?> GetLastLsnAsync(string captureInstance, CancellationToken cancellationToken = default)
    {
        _watermarks.TryGetValue(captureInstance, out var lsn);
        return Task.FromResult<byte[]?>(lsn is null ? null : lsn.ToArray());
    }

    /// <summary>
    /// Records the watermark, only ever forwards — the same guarantee <see cref="SqlCdcStateStore"/>
    /// enforces in SQL, so swapping stores cannot change what gets replayed.
    /// </summary>
    public Task SaveLastLsnAsync(string captureInstance, byte[] lsn, CancellationToken cancellationToken = default)
    {
        var candidate = lsn.ToArray();
        _watermarks.AddOrUpdate(
            captureInstance,
            candidate,
            (_, current) => LsnHelpers.Compare(candidate, current) > 0 ? candidate : current);

        return Task.CompletedTask;
    }
}

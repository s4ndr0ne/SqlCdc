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

    public Task SaveLastLsnAsync(string captureInstance, byte[] lsn, CancellationToken cancellationToken = default)
    {
        _watermarks[captureInstance] = lsn.ToArray();
        return Task.CompletedTask;
    }
}

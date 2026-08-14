namespace SqlCdc.IntegrationTests;

/// <summary>Polls a condition with a deadline, for state that settles asynchronously.</summary>
internal static class Wait
{
    public static readonly TimeSpan DefaultTimeout = TimeSpan.FromSeconds(30);

    public static Task<bool> UntilAsync(Func<bool> condition, TimeSpan? timeout = null) =>
        UntilAsync(() => Task.FromResult(condition()), timeout);

    public static async Task<bool> UntilAsync(Func<Task<bool>> condition, TimeSpan? timeout = null)
    {
        var deadline = DateTime.UtcNow + (timeout ?? DefaultTimeout);
        while (DateTime.UtcNow < deadline)
        {
            if (await condition())
            {
                return true;
            }

            await Task.Delay(100);
        }

        return await condition();
    }
}

namespace SqlCdc;

internal static class LsnHelpers
{
    /// <summary>Compares two LSN byte arrays (binary(10), big-endian).</summary>
    public static int Compare(byte[] a, byte[] b) => a.AsSpan().SequenceCompareTo(b);

    /// <summary>Returns the LSN immediately after the given one.</summary>
    public static byte[] Increment(byte[] lsn)
    {
        var result = (byte[])lsn.Clone();
        for (var i = result.Length - 1; i >= 0; i--)
        {
            if (++result[i] != 0)
            {
                break;
            }
        }

        return result;
    }
}

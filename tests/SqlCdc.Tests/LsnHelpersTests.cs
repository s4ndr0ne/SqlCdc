namespace SqlCdc.Tests;

public class LsnHelpersTests
{
    [Fact]
    public void Increment_AddsOneToLastByte()
    {
        var lsn = new byte[] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 };
        var result = LsnHelpers.Increment(lsn);
        Assert.Equal(0x01, result[^1]);
        Assert.Equal(0x00, result[0]);
    }

    [Fact]
    public void Increment_PropagatesCarry()
    {
        var lsn = new byte[] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xFF };
        var result = LsnHelpers.Increment(lsn);
        Assert.Equal(0x00, result[^1]);
        Assert.Equal(0x01, result[^2]);
    }

    [Fact]
    public void Compare_OrdersLsnAscending()
    {
        var low = new byte[] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01 };
        var high = new byte[] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02 };

        Assert.True(LsnHelpers.Compare(low, high) < 0);
        Assert.True(LsnHelpers.Compare(high, low) > 0);
        Assert.Equal(0, LsnHelpers.Compare(low, low));
    }

    [Fact]
    public void Increment_DoesNotMutateInput()
    {
        var lsn = new byte[] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01 };
        _ = LsnHelpers.Increment(lsn);
        Assert.Equal(0x01, lsn[^1]);
    }
}

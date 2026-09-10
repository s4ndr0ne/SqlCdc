namespace SqlCdc.Tests;

public class SqlCdcDeadLetterSinkTests
{
    private static CdcChange ChangeWith(Dictionary<string, object?> values) => new()
    {
        CaptureInstance = "dbo_Orders",
        SourceSchema = "dbo",
        SourceTable = "Orders",
        Operation = CdcOperationType.Insert,
        StartLsn = [0, 0, 0, 0, 0, 0, 0, 0, 0, 1],
        SeqVal = [0, 0, 0, 0, 0, 0, 0, 0, 0, 1],
        CommitTime = DateTime.UtcNow,
        Before = new Dictionary<string, object?>(),
        After = values,
        UpdateMask = new Dictionary<string, bool>(),
    };

    private sealed class SelfReferencingObject
    {
        public SelfReferencingObject? Self { get; set; }
    }

    private sealed class ThrowsOnPropertyAndToString
    {
        public string Boom => throw new InvalidOperationException("Boom in property");
        public override string ToString() => throw new InvalidOperationException("Boom in ToString");
    }

    [Fact]
    public void SerializePayload_WithNormalValues_ProducesValidJson()
    {
        var change = ChangeWith(new Dictionary<string, object?>
        {
            ["Id"] = 1,
            ["Name"] = "Widget",
        });

        var json = SqlCdcDeadLetterSink.SerializePayload(change);

        Assert.Contains("\"Name\":\"Widget\"", json);
        Assert.Contains("\"Id\":1", json);
    }

    [Fact]
    public void SerializePayload_WithSelfReferencingObject_DoesNotThrowAndFallsBack()
    {
        var cyclic = new SelfReferencingObject();
        cyclic.Self = cyclic;

        var change = ChangeWith(new Dictionary<string, object?>
        {
            ["Cyclic"] = cyclic,
        });

        // JsonSerializer.Serialize throws JsonException on cycle. It must not bubble up.
        var json = SqlCdcDeadLetterSink.SerializePayload(change);

        Assert.NotNull(json);
        Assert.Contains("SelfReferencingObject", json);
    }

    [Fact]
    public void SerializePayload_WhenPropertyAndToStringThrow_DoesNotThrowAndReportsFailure()
    {
        var change = ChangeWith(new Dictionary<string, object?>
        {
            ["Bad"] = new ThrowsOnPropertyAndToString(),
        });

        var json = SqlCdcDeadLetterSink.SerializePayload(change);

        Assert.NotNull(json);
        Assert.Contains("ToString failed", json);
    }
}

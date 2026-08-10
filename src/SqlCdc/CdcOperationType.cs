namespace SqlCdc;

/// <summary>
/// The type of database operation that produced a CDC change.
/// </summary>
public enum CdcOperationType
{
    /// <summary>An insert (captured as operation 2 in the CDC log).</summary>
    Insert = 2,

    /// <summary>An update (captured as operations 3/4, before/after, in the CDC log).</summary>
    Update = 4,

    /// <summary>A delete (captured as operation 1 in the CDC log).</summary>
    Delete = 1,
}

namespace SqlCdc;

internal static class SqlIdentifier
{
    public static string Quote(string identifier, string parameterName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(identifier, parameterName);

        if (identifier.Length > 128)
        {
            throw new ArgumentException("SQL identifiers cannot exceed 128 characters.", parameterName);
        }

        return $"[{identifier.Replace("]", "]]", StringComparison.Ordinal)}]";
    }
}

namespace SqlCdc.Tests;

public class SqlIdentifierTests
{
    [Theory]
    [InlineData("dbo", "[dbo]")]
    [InlineData("name]with]brackets", "[name]]with]]brackets]")]
    public void Quote_EscapesDelimitedIdentifier(string value, string expected)
    {
        Assert.Equal(expected, SqlIdentifier.Quote(value, "identifier"));
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    public void Quote_RejectsEmptyIdentifier(string value)
    {
        Assert.Throws<ArgumentException>(() => SqlIdentifier.Quote(value, "identifier"));
    }
}

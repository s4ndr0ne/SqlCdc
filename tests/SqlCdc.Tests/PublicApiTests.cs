using System.Runtime.CompilerServices;
using PublicApiGenerator;

namespace SqlCdc.Tests;

/// <summary>
/// Pins the public surface of the package. The point is not that the API never changes, but that
/// changing it shows up as a reviewable diff in the pull request rather than being noticed by a
/// consumer after release — package validation only catches breaks against the last published
/// version, which is too late to reconsider a name.
/// </summary>
public class PublicApiTests
{
    [Fact]
    public void ThePublicApi_MatchesTheApprovedFile()
    {
        var api = typeof(SqlCdcWatcher).Assembly.GeneratePublicApi(new ApiGeneratorOptions
        {
            // Assembly-level attributes carry the version, which MinVer changes on every commit.
            ExcludeAttributes =
            [
                "System.Reflection.AssemblyMetadataAttribute",
                "System.Runtime.Versioning.TargetFrameworkAttribute",
                "System.Runtime.CompilerServices.InternalsVisibleToAttribute",
            ],
        }).Replace("\r\n", "\n").TrimEnd();

        var baseName = Path.Combine(ProjectDirectory(), "PublicApi");
        var approvedPath = $"{baseName}.approved.txt";
        var approved = File.Exists(approvedPath)
            ? File.ReadAllText(approvedPath).Replace("\r\n", "\n").TrimEnd()
            : string.Empty;

        if (api == approved)
        {
            return;
        }

        // Written next to the approved file so accepting an intended change is a copy, and so the
        // full new surface is visible even when the assertion message truncates.
        var receivedPath = $"{baseName}.received.txt";
        File.WriteAllText(receivedPath, api + "\n");

        Assert.Fail(
            $"The public API changed. Review the diff and, if the change is intended, replace{Environment.NewLine}" +
            $"  {approvedPath}{Environment.NewLine}with{Environment.NewLine}  {receivedPath}{Environment.NewLine}" +
            $"Remember to record anything breaking in CHANGELOG.md.");
    }

    private static string ProjectDirectory([CallerFilePath] string sourcePath = "") =>
        Path.GetDirectoryName(sourcePath)!;
}

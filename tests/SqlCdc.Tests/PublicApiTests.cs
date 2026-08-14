using System.Reflection;
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
    /// <summary>Lines of difference to include in the failure message before truncating.</summary>
    private const int MaxReportedDifferences = 40;

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
        });

        var directory = ApprovalDirectory();
        var approvedPath = Path.Combine(directory, "PublicApi.approved.txt");

        Assert.True(
            File.Exists(approvedPath),
            $"The approved API file is missing at {approvedPath}. It is committed source, not a build output.");

        var approved = Normalise(File.ReadAllText(approvedPath));
        var actual = Normalise(api);
        if (actual == approved)
        {
            return;
        }

        // Written next to the approved file so accepting an intended change is a copy.
        var receivedPath = Path.Combine(directory, "PublicApi.received.txt");
        File.WriteAllText(receivedPath, actual + Environment.NewLine);

        Assert.Fail(
            $"The public API changed.{Environment.NewLine}{Environment.NewLine}" +
            $"{Describe(approved, actual)}{Environment.NewLine}{Environment.NewLine}" +
            $"If the change is intended, accept it with:{Environment.NewLine}" +
            $"  cp \"{receivedPath}\" \"{approvedPath}\"{Environment.NewLine}" +
            "and record anything breaking in CHANGELOG.md.");
    }

    private static string Normalise(string api) => api.Replace("\r\n", "\n").TrimEnd();

    private static bool HasContent(string line) => !string.IsNullOrWhiteSpace(line);

    /// <summary>
    /// Summarises the change as added and removed lines. The generated API is sorted, so comparing
    /// it as a set of lines reads better than a positional diff and survives reordering.
    /// </summary>
    private static string Describe(string approved, string actual)
    {
        var before = approved.Split('\n');
        var after = actual.Split('\n');

        var added = after.Except(before).Where(HasContent).Select(line => $"+ {line.Trim()}");
        var removed = before.Except(after).Where(HasContent).Select(line => $"- {line.Trim()}");

        var differences = removed.Concat(added).ToList();
        var reported = differences.Take(MaxReportedDifferences).ToList();
        if (differences.Count > reported.Count)
        {
            reported.Add($"... and {differences.Count - reported.Count} more");
        }

        return string.Join(Environment.NewLine, reported);
    }

    /// <summary>
    /// The project directory, injected by the build. Falls back to the output directory, where the
    /// approved file is copied, in case the assembly is ever run from somewhere else.
    /// </summary>
    private static string ApprovalDirectory()
    {
        var projectDirectory = typeof(PublicApiTests).Assembly
            .GetCustomAttributes<AssemblyMetadataAttribute>()
            .FirstOrDefault(attribute => attribute.Key == "ProjectDirectory")
            ?.Value;

        return projectDirectory is not null && Directory.Exists(projectDirectory)
            ? projectDirectory
            : AppContext.BaseDirectory;
    }
}

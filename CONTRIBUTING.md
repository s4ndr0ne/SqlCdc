# Contributing

## Getting set up

```bash
dotnet restore
dotnet build SqlCdc.slnx
dotnet test tests/SqlCdc.Tests               # unit tests, no external dependencies
dotnet test tests/SqlCdc.IntegrationTests    # requires Docker
```

The unit tests target `net8.0` and `net10.0`, because the library ships for both.

Running the `net8.0` set needs the .NET 8 **runtime** installed next to the .NET 10 SDK. Without
it `dotnet test` fails to launch that target and exits non-zero even though the `net10.0` tests
passed. Either install the runtime:

```bash
# https://dotnet.microsoft.com/download/dotnet/8.0, or
curl -sSL https://dot.net/v1/dotnet-install.sh | bash -s -- --runtime dotnet --channel 8.0
```

or run a single target while developing, and let CI cover the other:

```bash
dotnet test tests/SqlCdc.Tests -f net10.0
```

Integration tests start a SQL Server container with Testcontainers, with SQL Server Agent enabled
because the CDC capture job does not run without it. On Apple Silicon the image is amd64 and runs
under emulation, so Rosetta must be enabled in Docker Desktop. A full run takes roughly a minute
including container startup.

## Before opening a pull request

- `dotnet build` must be clean. Warnings are errors, so a new warning fails the build.
- Both test projects must pass.
- If you changed the public API, the `PublicApiTests` will fail with a diff. Review it, and if the
  change is intended replace `tests/SqlCdc.Tests/PublicApi.approved.txt` with the generated
  `PublicApi.received.txt` in the same folder.
- If the change breaks compatibility with the last published package, `dotnet pack` fails. That is
  the point: decide whether the break is worth it, record it under **Breaking** in
  `CHANGELOG.md`, and regenerate the suppression file with
  `dotnet pack src/SqlCdc/SqlCdc.csproj -p:ApiCompatGenerateSuppressionFile=true`.
- Add an entry to `CHANGELOG.md` under `[Unreleased]`.

## Style

The code aims to read as one piece: comments explain *why* a decision was made, not what a line
does, and are worth writing where the reason is not obvious from the code — a SQL Server quirk, a
race that is being avoided, a default that was chosen deliberately.

`.editorconfig` carries the formatting rules and your editor should apply them automatically.

## Releasing

The version is derived from the git tag by MinVer, so nothing in the repository records it.

1. Move `[Unreleased]` in `CHANGELOG.md` to the new version.
2. Tag: `git tag v1.2.3 && git push origin v1.2.3`.
3. The release workflow builds, tests, packs and creates the GitHub release with the `.nupkg` and
   `.snupkg` attached.
4. Publish to NuGet by hand, from the release assets or from a local pack:

   ```bash
   dotnet pack src/SqlCdc/SqlCdc.csproj -c Release -o ./artifacts
   dotnet nuget push ./artifacts/s4ndr0ne.SqlCdc.1.2.3.nupkg \
       --source https://api.nuget.org/v3/index.json --api-key <your key>
   ```

   Pushing the `.nupkg` uploads the matching `.snupkg` from the same folder automatically. Packing
   locally produces the same version as the tag, but without SourceLink pointing at a pushed
   commit — prefer the artifact built by the workflow.

After a release, raise `PackageValidationBaselineVersion` in `src/SqlCdc/SqlCdc.csproj` to the
version just published, and clear `src/SqlCdc/CompatibilitySuppressions.xml` — anything it still
listed has shipped and is now the baseline.

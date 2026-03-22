# Release Process

This document describes how to cut a new release of DSMySQL.

## Overview

Releases are driven entirely by git tags.  Pushing a tag of the form
`v<major>.<minor>.<patch>` triggers the [Release workflow](.github/workflows/release.yml),
which:

1. Validates that the tag matches the `VERSION` declared in `CMakeLists.txt`.
2. Runs the full unit-test suite (GCC).
3. Generates `version_generated.hpp` from the template.
4. Packages the public headers into `ds_mysql-v<version>.tar.gz`.
5. Creates a GitHub Release and attaches the archive.

The easiest way to perform all pre-tag steps in one shot is the helper script:

```bash
./scripts/release/release.sh <new-version>   # e.g. ./scripts/release/release.sh 1.1.0
```

---

## Step-by-step (manual)

### 1. Decide the new version

Follow [Semantic Versioning](https://semver.org):

| Change | Version component |
|--------|------------------|
| Backwards-incompatible API change | **major** |
| New backwards-compatible feature | **minor** |
| Bug fix / documentation / build fix | **patch** |

### 2. Update `CMakeLists.txt`

```cmake
project(DSMySQL
    VERSION <new-version>   # ← change this
    ...
)
```

The version is the single source of truth.

If you use `scripts/release/release.sh`, it also updates fallback constants in
`lib/include/ds_mysql/version.hpp` so raw-header/non-CMake consumers see the
new version too.

For a fully manual release, update `lib/include/ds_mysql/version.hpp` major,
minor, patch, and string values to match `<new-version>`.

### 3. Update `CHANGELOG.md`

Move items from `[Unreleased]` into a new section:

```markdown
## [<new-version>] – YYYY-MM-DD

### Added / Changed / Fixed / Removed
- ...

[<new-version>]: https://github.com/DisciplinedSoftware/DSMySQL/compare/v<previous>...v<new>
```

Also update the `[Unreleased]` comparison link at the bottom of the file.

### 4. Commit

```bash
git add CMakeLists.txt lib/include/ds_mysql/version.hpp CHANGELOG.md
git commit -m "chore: release v<new-version>"
```

### 5. Tag and push

```bash
git tag v<new-version>
git push origin main v<new-version>
```

The release workflow starts automatically when the tag is pushed.

---

## Verifying the release

After the workflow succeeds:

- The GitHub Release page lists the new tag with auto-generated release notes.
- The `ds_mysql-v<version>.tar.gz` archive is attached and contains every public
  header under a top-level `ds_mysql/` directory, including the generated
  `version_generated.hpp`.
- Confirm `ds_mysql::version::string` matches the new version by inspecting
  `version_generated.hpp` inside the archive.

---

## Hotfix releases

Hotfixes follow the same process but branch off the release tag:

```bash
git checkout -b hotfix/v<major>.<minor>.<patch+1> v<major>.<minor>.<patch>
# ... apply fix ...
git commit -m "fix: <description>"
# bump CMakeLists.txt VERSION and CHANGELOG, then:
git tag v<major>.<minor>.<patch+1>
git push origin hotfix/... v<major>.<minor>.<patch+1>
```

Merge the hotfix branch back to `main` afterwards.

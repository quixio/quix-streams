# Release Process

This document describes how to release a new version of quix-streams.

## Version Management

The version is stored in `quixstreams/__init__.py` as the `__version__` variable. This is dynamically read by `pyproject.toml` for package builds.

## Release Steps

### 1. Prepare the Release

1. Ensure you're on the `main` branch and it's up to date:
   ```bash
   git checkout main
   git pull origin main
   ```

2. Update the version in `quixstreams/__init__.py`:
   ```python
   __version__ = "X.Y.Z"
   ```

3. Commit the version bump:
   ```bash
   git add quixstreams/__init__.py
   git commit -m "Bump version to X.Y.Z"
   git push origin main
   ```

4. Create and push a git tag:
   ```bash
   git tag vX.Y.Z
   git push origin vX.Y.Z
   ```

### 2. PyPI Release

Build and upload the package to PyPI:

```bash
# Install build tools if needed
pip install build twine

# Build the package
python -m build

# Upload to PyPI
twine upload dist/*
```

### 3. Conda/Anaconda Release

Detailed instructions are in `conda/README.md`.

1. Get an Anaconda API Token from https://anaconda.org/quixio/settings/access

2. Test the build locally:
   ```bash
   ./conda/test.sh
   ```

3. Build and publish:
   ```bash
   ANACONDA_API_TOKEN='<your-token>' ./conda/release.sh
   ```

### 4. GitHub Release

Create a GitHub release from the tag at:
https://github.com/quixio/quix-streams/releases/new

## Installation Verification

After releasing, verify the package is available:

```bash
# PyPI
pip install quixstreams==X.Y.Z

# Conda
conda install -c conda-forge quixio::quixstreams=X.Y.Z
```

## Related Files

- `quixstreams/__init__.py` - Version definition
- `pyproject.toml` - PyPI package configuration
- `conda/meta.yaml` - Conda package metadata
- `conda/release.sh` - Conda release script

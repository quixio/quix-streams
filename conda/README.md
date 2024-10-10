# How to Build the Package and Publish to Anaconda.org

1. Make sure you are on `main` branch and it is up to date.
2. Get `quix` Anaconda API Token from https://anaconda.org/quixio/settings/access.
3. Test without uploading to Anaconda: `./conda/test.sh`
4. Build and publish: `ANACONDA_API_TOKEN='<Anaconda API Token>' ./conda/release.sh`

# How to upgrade requirements

1. Bump requirements in `conda/meta.yaml` and/or `conda/post-link.sh`.
2. Run `./conda/test.sh` to make sure everything works.

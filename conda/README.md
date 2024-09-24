# How to Build the Package and Publish to Anaconda.org

From the root of the repository, run:
```bash
docker run -v .:/quix-streams -w /quix-streams --rm -it continuumio/miniconda3 bash
```

Obtain Anaconda API Token from https://anaconda.org/quixio/settings/access.
Inside the container, execute the following commands:
```bash
export ANACONDA_API_TOKEN='<Anaconda API Token>'
export QUIXSTREAMS_VERSION=$(grep '__version__' ./quixstreams/__init__.py | cut -d '"' -f 2)
conda install --yes conda-build anaconda-client
conda build --channel conda-forge --user quixio ./conda
```

# How to upgrade requirements

1. Bump requirements in `conda/meta.yaml` and/or in `conda/post-link.sh`.
2. Run `./conda/test.sh` to make sure everything works.

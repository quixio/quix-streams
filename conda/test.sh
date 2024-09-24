QUIXSTREAMS_VERSION=$(grep '__version__' ./quixstreams/__init__.py | cut -d '"' -f 2)

docker run --rm \
-v ./conda:/conda \
-w /conda \
-e QUIXSTREAMS_VERSION=$QUIXSTREAMS_VERSION \
continuumio/miniconda3 bash -c 'conda install --yes conda-build anaconda-client && conda build --channel conda-forge .'

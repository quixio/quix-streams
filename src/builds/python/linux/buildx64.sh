#!/bin/bash

source ./buildx64_python3810.sh # we need this for ubuntu 20.04 on amdx64 at least
if [[ $? != 0 ]]; then exit $?; fi

docker build . -f ./dockerfile_amd64 --tag quixstreams-build-manylinux-amd64
if [[ $? != 0 ]]; then exit $?; fi

docker run -i \
    -v "$(pwd)/../../..:/QuixStreams" \
    -v "$(pwd)/../../../../LICENSE:/QuixStreams/LICENSE" \
    -v "$(pwd)/build-result:/build-result" \
    quixstreams-build-manylinux-amd64

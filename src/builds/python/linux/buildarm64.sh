#!/bin/bash

docker build . -f ./dockerfile_arm64 --tag quixstreams-build-manylinux-arm64
if [[ $? != 0 ]]; then exit $?; fi

docker run -i \
    -v "$(pwd)/../../..:/QuixStreams" \
    -v "$(pwd)/../../../../LICENSE:/QuixStreams/LICENSE" \
    -v "$(pwd)/build-result:/build-result" \
    quixstreams-build-manylinux-arm64
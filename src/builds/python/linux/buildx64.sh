#!/bin/bash

docker build . -f ./dockerfile_amd64 --tag quixstreams-build-manylinux-amd64
if [[ $? != 0 ]]; then exit $?; fi

docker run -i \
    -v "$(pwd)/../../..:/Quix.Streams" \
    -v "$(pwd)/../../../../LICENSE:/Quix.Streams/LICENSE" \
    -v "$(pwd)/build-result:/build-result" \
    quixstreams-build-manylinux-amd64
#!/bin/bash

docker build . -f ./dockerfile_amd64 --tag quixstreams-build-manylinux-amd64
if [[ $? != 0 ]]; then exit $?; fi

docker run -i \
    -v "$(pwd)/../../..:/Quix.Sdk" \
    -v "$(pwd)/../../../../LICENSE:/Quix.Sdk/LICENSE" \
    -v "$(pwd)/build-result:/build-result" \
    quixstreams-build-manylinux-amd64
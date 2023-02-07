#!/bin/bash

cd /Quix.Sdk/builds/python/linux
chmod +x ./build_native.sh
./build_native.sh
if [[ $? != 0 ]]; then exit $?; fi
chmod +x ./build_wheel.sh
./build_wheel.sh
if [[ $? != 0 ]]; then exit $?; fi

mkdir -p /build-result
pythonfolder=../../../PythonClient
cd $pythonfolder
\cp $(find ./dist | grep .whl) /build-result
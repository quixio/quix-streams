#!/bin/bash

sourcefolder=$(pwd)/build-result/python3.8.10
fileexpected="libpython3.8.so.1.0"
pythonfolder="../../../PythonClient"
nativefolder="$pythonfolder/src/quixstreams/native/libpython"
if [ -e "$sourcefolder/$fileexpected" ]; then
  echo "Python 3.8.10 is already compiled"
else
  echo "Compiling python 3.8.10 as it does not exist yet"
  rm -rf $nativefolder # in case we have something there, delete as we're rebuilding
  mkdir -p "$sourcefolder"

  docker build . -f ./dockerfile_amd64_python --build-arg PYVER=3.8.10 --tag build-manylinux-amd64-python3-8-10
  if [[ $? != 0 ]]; then exit $?; fi

  docker run -i \
    -v "$sourcefolder:/build-result" \
    build-manylinux-amd64-python3-8-10
fi

if [ -e "$nativefolder/$fileexpected" ]; then
  return 0
fi

mkdir -p "$nativefolder"
\cp $(find $sourcefolder -maxdepth 1 -type f) $nativefolder

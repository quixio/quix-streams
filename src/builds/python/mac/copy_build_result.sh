#!/bin/zsh

pythonwheeldest=$(pwd)/build-result
mkdir -p $pythonwheeldest
pythonfolder=../../../PythonClient
cd $pythonfolder
find ./dist | grep .whl
cp $(find ./dist | grep .whl) $pythonwheeldest
#!/bin/bash
pythonfolder=../../../PythonClient
cd $pythonfolder
rm -rf "./build"
rm -rf "./dist"
rm -rf "./src/quixstreams.egg-info"

archname=$(uname -m)
if [ $archname == 'aarch64' ]; then
    platname=manylinux_2_28_aarch64
elif [ $archname == 'x86_64' ]; then
    platname=manylinux2014_x86_64
else
  echo "Not yet supported architecture $archname"
  return 1
fi

python3 setup.py sdist bdist_wheel --plat-name $platname
#!/bin/zsh

pythonfolder=../../../PythonClient
cd $pythonfolder
rm -rf "./build"
rm -rf "./dist"
rm -rf "./src/quixstreams.egg-info"

archname=$(uname -m)
if [[ $archname == 'arm64' ]]; then
    # based on python3 -m pip debug --verbose | grep arm64
    platname=macosx_11_0_arm64
elif [[ $archname == 'x86_64' ]]; then
    # https://learn.microsoft.com/en-us/dotnet/core/rid-catalog
    # we build it using osx-x64, which has (Minimum OS version is macOS 10.12 Sierra)
    # therefore defaulting to  
    platname=macosx_10_12_intel
else
  echo "Not yet supported architecture $archname"
  return 1
fi

python3 setup.py sdist bdist_wheel --plat-name $platname
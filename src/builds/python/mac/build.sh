#!/bin/zsh

buildstartingpath=$(pwd)
source ./install_dependencies.sh
if [[ $? != 0 ]]; then exit $?; fi
cd $buildstartingpath
source ./build_native.sh
if [[ $? != 0 ]]; then exit $?; fi
cd $buildstartingpath
source ./build_wheel.sh
if [[ $? != 0 ]]; then exit $?; fi
cd $buildstartingpath
source ./copy_build_result.sh
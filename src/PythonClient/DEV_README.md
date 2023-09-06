# Who this readme is aimed at
If you're developing the library and wish to build locally, this readme is for you. Otherwise, check out 
https://test.pypi.org/project/quixstreams/ for latest ongoing dev versions or https://pypi.org/project/quixstreams/ for latest stable versions.

# Make sure you have python
We tested with minimum of 3.8.6 installed up to 3.11. Latest 3.x is probably fine.

# Build from source
Our builds are done using python, and you will find the builds in ../builds/python. Generally you will find 4 python scripts.
- one to install dependencies (for windows it is manual step of installing .net SDK)
- one to build the native code
- one to build the wheel
- one to build all

The native build takes several parameters. These can be found in each script, but generally support debugging such as partial build to maintain some manual modifications.

## Windows
Windows build does not yet have a docker to run it in, therefor you will need a Windows x86_64 machine to build it.

## Linux (x86_64 and arm64)
These use docker images to build, so as long as you have docker, the build scripts should work. There is no native linux build if your OS is already that, but feel free to contribute one.

### WSL approach
If you have windows and having issues, there are some common fixes described below:

#### Docker run just quits
Based on the resources linked below, you'll have to enable vsyscall=emulate
https://mail.python.org/pipermail/wheel-builders/2016-December/000239.html
(WSL) https://github.com/microsoft/WSL/issues/4694

For WSL, you enable it by editing your WSL config and restarting it:
```
%userprofile%\.wslconfig

[wsl2]
kernelCommandLine = vsyscall=emulate
```

To restart WSL:
```
wsl --shutdown
```


## Mac (Intel and Apple silicon)
Both Intel and Apple silicons are supported as of 0.5.2, previously only Intel. There is no docker build for this, so your build machine must be mac of the desired architecture.

# Run pypiserver locally to test hosting

Start your pypi server
```
docker run -p 8080:8080 pypiserver/pypiserver  -P . -a . --disable-fallback
```

You can verify it is running correctly by going to `http://localhost:8080/`.

Upload wheel files from a folder:
```
twine upload -p . -u . --skip-existing -r local --repository-url http://localhost:8080 ./*
```

You can use the website (`http://localhost:8080/simple/`) or the command below the list packages that you installed
```
pip search --index=http://localhost:8080 quixstreams
```

Install using the local registry:
```
python3 -m pip install quixstreams --index=http://localhost:8080/simple --extra-index=https://pypi.org/simple
```
Note: If you want to make sure the package works on its own, suggest `brew uninstall librdkafka`

# Test wheel

## Test content
The absolute minimum to test the installed package use the following `.py` file content:
```
echo "import quixstreams as qx" >> main.py
echo "client = qx.KafkaStreamingClient('127.0.0.1:9092', None)" >> main.py
echo "client.get_topic_producer('testinstall')" >> main.py
python3 main.py
```
Note: unless you have a local kafka running, this is expected to fail by logging Kafka producer exceptions.
Note2: On OSX, `DYLD_PRINT_APIS=1 DYLD_PRINT_LIBRARIES=1 python3 main.py` can be helpful for debugging load errors

## Test linux-x86_64
```
docker run -it \
    --platform=linux/amd64 \
    --entrypoint /bin/bash \
    --network=host \
    --volume /YOURSOURCEPATH/QuixStreams/builds/python/linux/build-result:/build-result \
    --volume /YOURSOURCEPATH/QuixStreams/PythonClient/tests/quixstreams/manual:/manual \
    python:3.11.1-slim-buster
```

## Test linux-arm64
```
docker run -it \
    --platform=linux/arm64/v8 \
    --entrypoint /bin/bash \
    --network=host \
    python:3.11.1-slim-buster
```
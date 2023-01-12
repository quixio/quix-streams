FROM ubuntu:20.04 as base

ENV DEBIAN_FRONTEND="noninteractive"
ENV PYTHONUNBUFFERED=1
ENV PYTHONIOENCODING=UTF-8
# Install python and some utilities
RUN apt-get update && \
    apt-get install -y curl && \
    apt-get install -y python3.8 python3.8-distutils && \
    curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py && \
    python3 get-pip.py && \
#install mono (for pythonnet)
    apt-get install -y gnupg ca-certificates && \
    apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 3FA7E0328081BFF6A14DA29AA6A19B38D3D831EF && \
    echo "deb https://download.mono-project.com/repo/ubuntu stable-focal/snapshots/6.10.0 main" | tee /etc/apt/sources.list.d/mono-official-stable.list && apt-get update && \
    apt-get install -y mono-devel && \
# Workaround an issue with certs due to mono BTLS
    sed -i 's#mozilla/DST_Root_CA_X3.crt#!mozilla/DST_Root_CA_X3.crt#' /etc/ca-certificates.conf && update-ca-certificates && \
# install additional pythonnet dependencies
    apt-get install -y clang curl python3-dev libglib2.0 build-essential && \
    python3 -m pip install pycparser && \
# install generic dependencies of SDK to reduce build times
    python3 -m pip install pandas==1.2.1 pythonnet==2.5.1

# split to avoid rebuilding the first half in case something isn't right
FROM base
# Install librdkafka dependencies for ubuntu 20.04
COPY ./kafkadepinstaller.sh .
RUN chmod +x ./kafkadepinstaller.sh && \
    . ./kafkadepinstaller.sh && \
    rm /kafkadepinstaller.sh

#TODO: uncomment for DOTNET build
# RUN wget https://packages.microsoft.com/config/ubuntu/20.04/packages-microsoft-prod.deb -O packages-microsoft-prod.deb; \
#     dpkg -i packages-microsoft-prod.deb
# RUN apt-get update; \
#   apt-get install -y apt-transport-https dotnet-sdk-5.0

# #install dotnet runtime
# RUN wget https://dot.net/v1/dotnet-install.sh && chmod +x ./dotnet-install.sh && /bin/bash ./dotnet-install.sh -c Current && rm ./dotnet-install.sh

COPY ./lib ./pythonlib

#TODO: uncomment for DOTNET build
#build dotnet files
# WORKDIR /app/PythonClient
# RUN dotnet build ../CsharpClient/Quix.Sdk.Streaming/Quix.Sdk.Streaming.csproj /t:publish -c debug --no-restore /p:Configuration=PythonLocal /p:SelfContained=true /p:RuntimeIdentifier=linux-x64 /p:DebugType=None /p:PublishDir=../../PythonClient/lib/quixstreaming/dotnet/linux-x64
# WORKDIR /


#RUN pip3 install -e /app/PythonClient/lib

#RUN mkdir /app && mkdir /app/source
WORKDIR /pythonlib
#COPY ./PythonClient/lib/requirements.txt ./

#RUN ls -la
RUN pip3 install -r ./requirements.txt --extra-index-url https://pkgs.dev.azure.com/quix-analytics/53f7fe95-59fe-4307-b479-2473b96de6d1/_packaging/public/pypi/simple/

#ENTRYPOINT ["/bin/bash"]
ENTRYPOINT ["python3", "./mainv2.py"]
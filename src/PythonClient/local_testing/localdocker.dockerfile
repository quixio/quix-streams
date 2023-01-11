FROM base:1.0
WORKDIR /app
COPY /app .
WORKDIR /app/lib
RUN python3 -m pip install virtualenv && \
    python3 -m virtualenv env && \
    chmod +x ./env/bin/activate && \
    mv ./quixstreaming ./env/lib/python3.8/site-packages/ && \
    . ./env/bin/activate && \
    find | grep requirements.txt | xargs -I '{}' python3 -m pip install -r '{}' --extra-index-url https://pkgs.dev.azure.com/quix-analytics/53f7fe95-59fe-4307-b479-2473b96de6d1/_packaging/public/pypi/simple/
ENTRYPOINT ["bash"]

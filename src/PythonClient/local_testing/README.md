1) Build with localbuild.bat
2) Copy it to target machine (or not if testing on current OS)
3) if executed in PythonClient lib, using linux subsystem:
scp -r ./lib user@targetip:~/app/lib
4) remote onto target machine
5) Setup dependencies according to other readme (see sample projects)   
    Virtual env:
        python3 -m pip install virtualenv && \
        python3 -m virtualenv env && \
        chmod +x ./env/bin/activate && \
        # NOTE: for command below, use whichever python you have
        mv ./quixstreaming ./env/lib/python3.8/site-packages/ && \
        . ./env/bin/activate
        python3 -m pip install -r requirements.txt --extra-index-url https://pkgs.dev.azure.com/quix-analytics/53f7fe95-59fe-4307-b479-2473b96de6d1/_packaging/public/pypi/simple/

6) run. Following command will run in background thread, so even if stuck you can kil it. logs will be also visible in logs.txt
    python main.py 2>&1 | tee logs.txt &


Local testing via docker:
1) See the local docker dockerfile
2) get the dockerfile from Quix.Build\builds\dockerprebuild\python\dockerfile
3) builds that docker file... will take time. tag it with "base:1.0". Command:
    docker build . --tag base:1.0
4) Now copy the localdocker.dockerfile to the VM/Agent etc you have and build it
   Note!!: A folder may only have a single docker file as docker uses the entire folder for creating the docker context. This means, 1 dockerfile or context / folder. So don't build this in same folder with the dockerfile from 3) next to it. Command:
    docker build . --tag app:1.0
5) run it
    docker run -it app:1.0
6) once in 
    . ./env/bin/activate
7) then you can run the main.py app with
    python main.py 2>&1 | tee logs.txt &    
set pythonfolder=../../../PythonClient
if exist %pythonfolder%/build rmdir /s /q "%pythonfolder%/build"
if exist %pythonfolder%/dist rmdir /s /q "%pythonfolder%/dist"
if exist %pythonfolder%/src/quixstreams.egg-info rmdir /s /q "%pythonfolder%/src/quixstreams.egg-info"
START /wait /b /D "%pythonfolder%" python setup.py sdist bdist_wheel --plat-name win_amd64
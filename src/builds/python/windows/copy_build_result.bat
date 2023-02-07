set pythonfolder=../../../PythonClient
set dest="./build-result/"
if exist %dest% rmdir /s /q %dest%
mkdir %dest%
xcopy /E /Y /D /I "%pythonfolder%\dist\*.whl" %dest%
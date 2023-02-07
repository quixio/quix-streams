set mypath=%cd%
call install_dependencies.bat
if %ERRORLEVEL% NEQ 0 exit /b %ERRORLEVEL%
cd %mypath%
call build_native.bat %*
if %ERRORLEVEL% NEQ 0 exit /b %ERRORLEVEL%
cd %mypath%
call build_wheel.bat
if %ERRORLEVEL% NEQ 0 exit /b %ERRORLEVEL%
cd %mypath%
call copy_build_result.bat
if %ERRORLEVEL% NEQ 0 exit /b %ERRORLEVEL%
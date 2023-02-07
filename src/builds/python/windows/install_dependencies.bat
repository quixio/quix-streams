python -m pip show wheel
if %ERRORLEVEL% NEQ 0 (
	python -m pip install wheel
)
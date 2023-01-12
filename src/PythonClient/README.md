# Requirements to run
## Python
Currently tested to work with [3.8.7](https://www.python.org/downloads/release/python-387/), but other python versions of 3.7 and 3.8 re expected to also work.

## Windows
- Install latest .Net Core runtime (https://dotnet.microsoft.com/download/dotnet-core/current/runtime)


## Create new python environment
Highly suggest to use a python virtual environment, as the Quix streaming package currently relies on some dll rebinding, which is achieved by adding a file to your python environment. This is done automatically, but to avoid any complication with other python applications you might want to use relying on similar techniques, a virtual environment is advised.

To create a new virtual environment
- Windows:
    ```
    # Note: You might need to restart he console after installing something, like python or virtual env
    python -m pip install virtualenv
    python -m virtualenv env
    call env/Scripts/activate
    ```

## Install samples requirements
- Windows:
    ```
    # in the folder where requirements.txt is located:
    pip install -r requirements.txt
    ```
## Run the sample
### From terminal
- Windows:
    ```
    python main.py
    ```

### From PyCharm
1) File > Settings
2) Search 'interpreter', find 'Python Interpreter' under project's name (Name will be something like 'Project: 1-hello-world')
3) Click on cog icon (around top right next to a dropdown)
4) Click on Add...
5) Under 'Virtual Environment' menu options, you'll have to option to select New Environment or Existing Environment. Select Existing and set interpreter path to python.exe in the virtual environment you created
6) click OK
7) Verify the newly added interpreter is selected as Python Interpreter (dropdown next to the cog in point 3)
8) Click OK
9) Right click on main.py, "Run 'main.py'"    
# The version will get automatically updated as part of setup
__version__ = "local"

import ctypes
import os
import platform
import sys

from .native.Python.InteropHelpers.InteropUtils import InteropUtils

plat = platform.uname()
platname = f"{plat.system}-{plat.machine}".lower()
lib_dir = os.path.join(os.path.dirname(__file__), "./native/" + platname + "/QuixStreams.Streaming.Interop/")
if plat.system.upper() == "WINDOWS":
    allowed_extensions = [".dll"]
    lib_dll = "QuixStreams.Streaming.Interop.dll"
    for file in os.listdir(lib_dir):
        if file == lib_dll:
            continue
        allowed = False
        for ext in allowed_extensions:
            if file.endswith(ext):
                allowed = True
                break
        if not allowed:
            continue
        ctypes.cdll.LoadLibrary(lib_dir + file)
elif plat.system.upper() == "DARWIN":
    allowed_extensions = [".dylib"]
    lib_dll = "QuixStreams.Streaming.Interop.dylib"
elif plat.system.upper() == "LINUX":
    allowed_extensions = [".so"]
    lib_dll = "QuixStreams.Streaming.Interop.so"
else:
    raise Exception("Platform {} is not supported".format(plat))

lib = ctypes.cdll.LoadLibrary(lib_dir + lib_dll)
InteropUtils.set_lib(lib)

env_debug = os.environ.get("QuixStreams__Debug")
if env_debug is not None and env_debug == '1':
    InteropUtils.enable_debug()

use_python_lib_path = os.environ.get("QuixStreams__PythonLibPath")
if use_python_lib_path is not None:
    InteropUtils.log_debug(f"QuixStreams__PythonLibPath is set to {use_python_lib_path}")
    libpython_dir = use_python_lib_path
    InteropUtils.set_python_lib_path(libpython_dir)
else:
    use_included = False
    if plat.system.upper() == "LINUX":
        if sys.version_info.major == 3 and sys.version_info.minor == 8:
            # on linux, 3.8.6 and 3.8.10, available from main distros for ubuntu 20.04 for example are not working as expected
            use_included = True

    # option to override
    use_included_env = os.environ.get("QuixStreams__UseIncludedPython")
    if use_included_env is not None:
        InteropUtils.log_debug(f"QuixStreams__UseIncludedPython is set to '{use_included_env}'")
        use_included = use_included_env == '1'

    if use_included:
        InteropUtils.log_debug(f"Using included python.")
        libpython_dir = os.path.join(os.path.dirname(__file__), "native/libpython")
        InteropUtils.set_python_lib_path(libpython_dir)

from .models import *
from .quixstreamingclient import QuixStreamingClient
from .streamconsumer import StreamConsumer
from .app import App, CancellationTokenSource, CancellationToken
from .kafkastreamingclient import KafkaStreamingClient
from .raw import *

from .topicproducer import TopicProducer

from .configuration import *
from .topicconsumer import TopicConsumer
from .streamproducer import StreamProducer

from .logging import Logging, LogLevel
from .state import *

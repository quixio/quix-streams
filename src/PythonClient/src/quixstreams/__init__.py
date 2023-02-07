# The version will get automatically updated as part of setup
__version__ = "local"

import os
import platform
import sysconfig
import ctypes

from .native.Python.InteropHelpers.InteropUtils import InteropUtils


plat = platform.uname()
platname = f"{plat.system}-{plat.machine}".lower()
lib_dir = os.path.join(os.path.dirname(__file__), "./native/" + platname + "/Quix.Sdk.Streaming.Interop/")
if plat.system.upper() == "WINDOWS":
    allowed_extensions = [".dll"]
    lib_dll = "Quix.Sdk.Streaming.Interop.dll"
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
    lib_dll = "Quix.Sdk.Streaming.Interop.dylib"
elif plat.system.upper() == "LINUX":
    allowed_extensions = [".so"]
    lib_dll = "Quix.Sdk.Streaming.Interop.so"
else:
    raise Exception("Platform {} is not supported".format(plat))

lib = ctypes.cdll.LoadLibrary(lib_dir + lib_dll)
InteropUtils.set_lib(lib)


from .models import *
from .quixstreamingclient import QuixStreamingClient
from .streamreader import StreamReader
from .app import App, CancellationTokenSource, CancellationToken
from .kafkastreamingclient import KafkaStreamingClient
from .raw import RawMessage

from .outputtopic import OutputTopic

from .configuration import *
from .inputtopic import InputTopic
from .streamwriter import StreamWriter

from .logging import Logging, LogLevel
from .state import LocalFileStorage

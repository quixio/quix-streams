# The version will get automatically updated as part of setup
__version__ = "local"

import ctypes
import os
import platform

from .native.Python.InteropHelpers.InteropUtils import InteropUtils

plat = platform.uname()
platname = f"{plat.system}-{plat.machine}".lower()
lib_dir = os.path.join(os.path.dirname(__file__), "./native/" + platname + "/Quix.Streams.Streaming.Interop/")
if plat.system.upper() == "WINDOWS":
    allowed_extensions = [".dll"]
    lib_dll = "Quix.Streams.Streaming.Interop.dll"
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
    lib_dll = "Quix.Streams.Streaming.Interop.dylib"
elif plat.system.upper() == "LINUX":
    allowed_extensions = [".so"]
    lib_dll = "Quix.Streams.Streaming.Interop.so"
else:
    raise Exception("Platform {} is not supported".format(plat))

lib = ctypes.cdll.LoadLibrary(lib_dir + lib_dll)
InteropUtils.set_lib(lib)

from .models import *
from .quixstreamingclient import QuixStreamingClient
from .streamconsumer import StreamConsumer
from .app import App, CancellationTokenSource, CancellationToken
from .kafkastreamingclient import KafkaStreamingClient
from .raw import RawMessage

from .topicproducer import TopicProducer

from .configuration import *
from .topicconsumer import TopicConsumer
from .streamproducer import StreamProducer

from .logging import Logging, LogLevel
from .state import *

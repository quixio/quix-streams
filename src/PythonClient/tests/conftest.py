import src.quixstreams as qx

# Uncomment the next lines to enable low-level interop logs
# It will also create a log file
# from src.quixstreams.native.Python.InteropHelpers.InteropUtils import InteropUtils
# InteropUtils.enable_debug()

qx.Logging.update_factory(qx.LogLevel.Critical)

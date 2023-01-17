from typing import Callable

from . import __importnet
import Quix.Sdk.Streaming
import clr
clr.AddReference("System.Threading")
import System.Threading

class App():
    """
        Helper class to handle default streaming behaviours and handle automatic resource cleanup on shutdown
    """

    @staticmethod
    def run(*args, before_shutdown: Callable[[], None] = None):
        """
            Helper method to handle default streaming behaviours and handle automatic resource cleanup on shutdown
            It also ensures input topics defined at the time of invocation are opened for read.

            :param before_shutdown: An optional function to call before shutting down
        """

        callback = None
        if before_shutdown is not None:
            callback = System.Action(before_shutdown)

        try:
            token = System.Threading.CancellationToken()
            if len(args) > 0:
                arg_token = args[0]
                if arg_token is not None and isinstance(arg_token, System.Threading.CancellationToken):
                    token = arg_token
            Quix.Sdk.Streaming.App.Run(token, callback)
        except KeyboardInterrupt:
            pass

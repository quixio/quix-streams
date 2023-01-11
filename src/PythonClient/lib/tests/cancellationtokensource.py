import clr
clr.AddReference('System.Threading')
import System.Threading

class CancellationTokenSource:
    """
        Signals to a System.Threading.CancellationToken that it should be canceled.
    """

    def __init__(self):
        """
            Creates a new instance of CancellationTokenSource\
        """
        self.__wrapped = System.Threading.CancellationTokenSource()

    def is_cancellation_requested(self):
        return self.__wrapped.IsCancellationRequested

    def cancel(self):
        self.__wrapped.Cancel()

    @property
    def token(self):
        return self.__wrapped.Token
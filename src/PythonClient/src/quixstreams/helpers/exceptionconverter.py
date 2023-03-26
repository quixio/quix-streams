from ..exceptions.quixapiexception import QuixApiException
from ..native.Python.InteropHelpers.InteropUtils import InteropException


class ExceptionConverter:

    @staticmethod
    def raise_from_interop(exception: InteropException):
        if exception.exc_type == "QuixStreams.Streaming.QuixApi.QuixApiException":
            raise QuixApiException(exception.message)

        if exception.exc_type == "System.NotImplementedException":
            raise NotImplementedError(exception.message + "\n" + exception.exc_stack)

        if exception.exc_type == "System.Exception":
            raise Exception(exception.message + "\n" + exception.exc_stack)

        raise exception  # when not able to do better

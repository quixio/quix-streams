from ..exceptions.quixapiexception import QuixApiException
from ..native.Python.InteropHelpers.InteropUtils import InteropException


class ExceptionConverter:

    @staticmethod
    def raise_from_interop(exception: InteropException):
        if exception.exc_type == "QuixStreams.Streaming.QuixApi.QuixApiException":
            raise QuixApiException(exception.message) from None  # using from None, as the original exception should be completely discarded

        raise  # when not able to do better

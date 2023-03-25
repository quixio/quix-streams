from ..exceptions.quixapiexception import QuixApiException
from ..native.Python.InteropHelpers.InteropUtils import InteropException


class ExceptionConverter:

    @staticmethod
    def raise_from_interop(exception: InteropException):
        if exception.exc_type == "QuixStreams.Streaming.QuixApi.QuixApiException":
            # using from None, as the original exception should be completely discarded
            raise QuixApiException(exception.message) from None

        raise  # when not able to do better

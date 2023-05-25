from .istatestorage import IStateStorage
from ..native.Python.QuixStreamsState.Storage.InMemoryStorage import InMemoryStorage as lmsi


class InMemoryStorage(IStateStorage):
    """
    Basic non-thread safe in-memory storage implementing IStateStorage
    """

    def __init__(self):
        """
        Initializes the InMemoryStorage instance.
        """
        super().__init__(lmsi.Constructor())


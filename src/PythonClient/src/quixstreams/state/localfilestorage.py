from .istatestorage import IStateStorage
from ..native.Python.QuixStreamsState.Storage.FileStorage.LocalFileStorage.LocalFileStorage import LocalFileStorage as lfsi


class LocalFileStorage(IStateStorage):
    """
    A directory storage containing the file storage for single process access purposes.
    Locking is implemented via in-memory mutex.
    """

    def __init__(self, storage_directory=None, auto_create_dir=True):
        """
        Initializes the LocalFileStorage instance.

        Args:
            storage_directory: The path to the storage directory.
            auto_create_dir: If True, automatically creates the storage directory if it doesn't exist.
        """
        super().__init__(lfsi.Constructor(storage_directory, auto_create_dir))

from .istatestorage import IStateStorage
from ..native.Python.QuixStreamsState.Storage.RocksDbStorage import RocksDbStorage as rdbsi


class RocksDbStorage(IStateStorage):
    """
    Key/Value storage using RocksDB.
    """

    def __init__(self, db_directory, storage_name="default-storage"):
        """
        Initializes the LocalFileStorage instance.

        Args:
            db_directory: The directory for storing the states.
            storage_name: Name of the storage. Used to separate data if other storages use the same database
        """
        super().__init__(rdbsi.Constructor(db_directory, storage_name))

from .basefilestorage import BaseFileStorage
from .. import __importnet
import Quix.Sdk.State

class LocalFileStorage(BaseFileStorage):

    def __init__(self, storageDirectory = None, autoCreateDir = True):
        """
            Initializes the LocalFileStorage containing the whole
        """
        super().__init__(Quix.Sdk.State.Storage.FileStorage.LocalFileStorage.LocalFileStorage(storageDirectory, autoCreateDir))

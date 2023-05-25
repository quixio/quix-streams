using QuixStreams.State.Storage;
using QuixStreams.State.Storage.FileStorage;
using QuixStreams.State.Storage.FileStorage.LocalFileStorage;

namespace QuixStreams.State.UnitTests
{
    public class LocalCRUDShould : BaseCRUDShould
    {
        override
        protected BaseFileStorage GetStorage()
        {
            var storage = new LocalFileStorage();
            storage.Clear();
            return storage;
        }
    }
}

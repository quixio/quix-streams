using Microsoft.VisualStudio.TestTools.UnitTesting;
using QuixStreams.State.Storage;
using QuixStreams.State.Storage.FileStorage;
using QuixStreams.State.Storage.FileStorage.LocalFileStorage;

namespace QuixStreams.State.UnitTests
{
    [TestClass]
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

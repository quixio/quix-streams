using Microsoft.VisualStudio.TestTools.UnitTesting;
using Quix.Streams.State.Storage;
using Quix.Streams.State.Storage.FileStorage;
using Quix.Streams.State.Storage.FileStorage.LocalFileStorage;

namespace Quix.Streams.State.UnitTests
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

using Microsoft.VisualStudio.TestTools.UnitTesting;
using Quix.Sdk.State.Storage;
using Quix.Sdk.State.Storage.FileStorage;
using Quix.Sdk.State.Storage.FileStorage.LocalFileStorage;

namespace Quix.Sdk.State.UnitTests
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

using Quix.TestBase.Extensions;
using QuixStreams.State.Storage;
using QuixStreams.State.Storage.FileStorage;
using QuixStreams.State.Storage.FileStorage.LocalFileStorage;
using Xunit.Abstractions;

namespace QuixStreams.State.UnitTests
{
    public class LocalCRUDShould : BaseCRUDShould
    {
        public LocalCRUDShould(ITestOutputHelper output) : base(output)
        {
        }
        
        protected override BaseFileStorage GetStorage()
        {
            var storage = new LocalFileStorage(loggerFactory:this.output.CreateLoggerFactory());
            storage.Clear();
            return storage;
        }
    }
}

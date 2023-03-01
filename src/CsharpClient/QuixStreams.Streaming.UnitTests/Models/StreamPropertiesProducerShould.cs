using System.Threading;
using System.Threading.Tasks;
using NSubstitute;
using Quix.TestBase.Extensions;
using QuixStreams.Streaming.Models.StreamProducer;
using Xunit;
using Xunit.Abstractions;

namespace QuixStreams.Streaming.UnitTests.Models
{
    public class StreamPropertiesProducerShould
    {
        private readonly ITestOutputHelper wrappingHelper;

        public StreamPropertiesProducerShould(ITestOutputHelper outputHelper)
        {
            this.wrappingHelper = Substitute.For<ITestOutputHelper>();
            this.wrappingHelper.When(y=> y.WriteLine(Arg.Any<string>())).Do(ci =>
            {
                outputHelper.WriteLine(ci.Arg<string>());
            });
            this.wrappingHelper.When(y=> y.WriteLine(Arg.Any<string>(), Arg.Any<object[]>())).Do(ci =>
            {
                outputHelper.WriteLine(ci.Arg<string>(), ci.Arg<object[]>());
            });
            
            Logging.Factory = outputHelper.CreateLoggerFactory();
        }
        
        [Fact]
        public void AutomaticIntervalledFlush_ShouldNotThrowException()
        {
            // Arrange
            var internalWriter = Substitute.For<IStreamProducerInternal>();
            var writer = new StreamPropertiesProducer(internalWriter);
            // Act
            var cts = new CancellationTokenSource(2000);
            var index = 0;
            Task.Run(() =>
            {
                while (!cts.IsCancellationRequested)
                {
                    writer.Flush();
                }
            });
            while (!cts.IsCancellationRequested)
            {
                writer.Metadata["test" + index] = index.ToString();
                writer.Parents.Add("1");
                index++;
            }
            
            // Assert by no exception
        }
    }
}
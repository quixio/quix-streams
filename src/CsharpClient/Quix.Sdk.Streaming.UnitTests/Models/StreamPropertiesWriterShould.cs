using System;
using System.Threading;
using System.Threading.Tasks;
using NSubstitute;
using Quix.Sdk.Streaming.Models.StreamWriter;
using Quix.TestBase.Extensions;
using Xunit;
using Xunit.Abstractions;

namespace Quix.Sdk.Streaming.UnitTests.Models
{
    public class StreamPropertiesWriterShould
    {
        private readonly ITestOutputHelper wrappingHelper;

        public StreamPropertiesWriterShould(ITestOutputHelper outputHelper)
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
            var internalWriter = Substitute.For<IStreamWriterInternal>();
            var writer = new StreamPropertiesWriter(internalWriter);
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
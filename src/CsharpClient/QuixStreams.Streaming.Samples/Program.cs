using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using QuixStreams.Streaming.Samples.Samples;

namespace QuixStreams.Streaming.Model.Samples
{
    class Program
    {
        private static CancellationTokenSource cts = new CancellationTokenSource();

        private static void Main(string[] args)
        {
            var cts = new CancellationTokenSource();
            Console.CancelKeyPress += (s, e) =>
            {
                if (cts.IsCancellationRequested) return;
                Console.WriteLine("Cancelling....");
                e.Cancel = true;
                cts.Cancel();
            };
            
            Logging.UpdateFactory(LogLevel.Debug);

           // ExampleReadWriteMessages(cts.Token);
           //ExampleReadWriteMessagesV2(cts.Token);
            
           // ExampleReadWriteWithManualCommitMessages(cts.Token);
           // ExampleReadWriteMessagesWithTimeout(cts.Token);

           ExampleReadWriteUsingQuixStreamingClient(cts.Token);
        }

        private static void ExampleReadWriteWithManualCommitMessages(in CancellationToken ctsToken)
        {
            new WriteSample().Start(ctsToken, Guid.NewGuid().ToString());
            new ReadSampleWithManualCommit().Start(ctsToken);
            try
            {
                Task.Delay(-1, ctsToken).GetAwaiter().GetResult();
            }
            catch
            {
            }
        }

        private static void ExampleReadWriteMessages(in CancellationToken ctsToken)
        {
            var stream = Guid.NewGuid().ToString();
            new WriteSample().Start(ctsToken, stream);
            var read = new ReadSample();
            read.Start(stream);
            try
            {
                Task.Delay(-1, ctsToken).GetAwaiter().GetResult();
            }
            catch
            {
            }
            read.Stop();
        }
        
        private static void ExampleReadWriteMessagesV2(in CancellationToken ctsToken)
        {
            var stream = Guid.NewGuid().ToString();
            new WriteSample().Start(ctsToken, stream);
            var read = new ReadSampleV2();
            read.Start(stream);
            App.Run();
        }
        
        private static void ExampleReadWriteMessagesWithTimeout(in CancellationToken ctsToken)
        {
            var stream = Guid.NewGuid().ToString();
            new WriteSample().Start(ctsToken, stream);
            var read = new ReadSampleWithTimeout();
            read.Start(stream);
            try
            {
                Task.Delay(-1, ctsToken).GetAwaiter().GetResult();
            }
            catch
            {
            }
            read.Stop();
        }

        private static void ExampleReadWriteUsingQuixStreamingClient(in CancellationToken cancellationToken)
        {
            var quixStreamClient = new QuixStreamingClient(QuixStreams.Streaming.Samples.Configuration.QuixStreamingClientConfig.Token);
            quixStreamClient.ApiUrl = new Uri(QuixStreams.Streaming.Samples.Configuration.QuixStreamingClientConfig.PortalApi);

            var topicProducer = quixStreamClient.GetTopicConsumer("iddqd");
        }
    }
}



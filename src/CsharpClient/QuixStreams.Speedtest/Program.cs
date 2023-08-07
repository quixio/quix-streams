using System;
using System.Threading;

namespace QuixStreams.Speedtest
{
    class Program
    {
        static void Main(string[] args)
        {
            var cts = new CancellationTokenSource();
            Console.CancelKeyPress += (s, e) =>
            {
                if (cts.IsCancellationRequested) return;
                Console.WriteLine("Cancelling....");
                e.Cancel = true;
                cts.Cancel();
            };
            
            // Uncomment this for testing Transport layer only
            //new TransportTest().Run(cts.Token);
            //new StreamingTestRaw().Run(cts.Token);
            
            // Uncomment this for testing Streaming and below layers
            new StreamingTest().Run(cts.Token);
//            new BufferTest().Run(cts.Token);
        }
    }
}
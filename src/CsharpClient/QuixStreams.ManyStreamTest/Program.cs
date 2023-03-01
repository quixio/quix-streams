using System;
using System.Threading;

namespace QuixStreams.ManyStreamTest
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
            
            // Uncomment this for testing Streaming and below layers
            new StreamingTest().Run(cts.Token);
        }
    }
}
using System;
using System.Threading;

namespace QuixStreams.ThroughputTest
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
            
            
            new StreamingTestRaw().Run(cts.Token, false);
        }
    }
}
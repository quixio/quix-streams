using System;
using System.Globalization;
using System.Threading;

namespace QuixStreams.ThroughputTest
{
    class Program
    {
        static void Main(string[] args)
        {
            CultureInfo ci = CultureInfo.InvariantCulture;
            Thread.CurrentThread.CurrentCulture = ci;
            Thread.CurrentThread.CurrentUICulture = ci;
            var cts = new CancellationTokenSource();
            Console.CancelKeyPress += (s, e) =>
            {
                if (cts.IsCancellationRequested) return;
                Console.WriteLine("Cancelling....");
                e.Cancel = true;
                cts.Cancel();
            };
            
            
            new StreamingTestRaw().Run(cts.Token);
        }
    }
}
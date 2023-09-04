using System;
using System.Threading;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using QuixStreams.Kafka.Transport.Samples.Samples;

namespace QuixStreams.Kafka.Transport.Samples
{
    internal class Program
    {
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

             //ExampleReadWriteMessages(cts.Token);

            // ExampleReadWritePackages(cts.Token);

            // ExampleReadFilteredWritePackages(cts.Token);

            // ExamplePackageReadWriteWithPartition(cts.Token);

            /// ExamplePackageReadWriteWithPartitionAndOffsets(cts.Token);

            // ExampleWithManualCommitForReadPackages(cts.Token);

            //ExampleReadWithoutConsumerGroup(cts.Token);

            // Error tests
            ExamplePackageReadWriteWithPartitionAndOffsetsTestTimeout(cts.Token);
            //ExampleReadWriteMessagesWithTimeout(cts.Token);
        }


        private static void ExamplePackageReadWriteWithPartition(CancellationToken ct)
        {
            // Example to read package from a specified partition
            new ReadPackageFromPartition().Start(new Partition(2), Offset.Unset);

            // Example to write package to a specified partition
            new WritePackageToPartition().Run(new Partition(2), ct);
        }

        private static void ExamplePackageReadWriteWithPartitionAndOffsets(CancellationToken ct)
        {
            // Example to read package from a specified partition
            new ReadPackageFromPartition().Start(new Partition(2), new Offset(10));

            // Example to write package to a specified partition
            new WritePackageToPartition().Run(new Partition(2), ct);
        }
        
        private static void ExamplePackageReadWriteWithPartitionAndOffsetsTestTimeout(CancellationToken ct)
        {
            // Example to read package from a specified partition
            new ReadPackageFromPartitionWithTimeout().Start(new Partition(0), Offset.Stored);

            // Example to write package to a specified partition
            new WritePackageToPartition().Run(new Partition(0), ct);
        }

        private static void ExampleWithManualCommitForReadPackages(CancellationToken ct)
        {
            new ReadPackageManualCommit().Start();

            new WritePackages().Run(ct);
        }

        private static void ExampleReadWritePackages(CancellationToken ct)
        {
            // Example for simple package (Typed messages) reading
            new ReadPackages().Start();

            // Example for simple package (Typed messages) writing
            new WritePackages().Run(ct);
        }

        private static void ExampleReadFilteredWritePackages(CancellationToken ct)
        {
            // Example for package (Typed messages) reading with filter
            new ReadFilteredPackages().Start();

            // Example for simple package (Typed messages) writing
            new WritePackages().Run(ct);
        }

        private static void ExampleReadWriteMessages(CancellationToken ct)
        {
            // Example for simple raw message read (lowest level possible)
            new ReadMessage().Start();

            // Example for simple raw message write (lowest level possible)
            new WriteMessage().Run(ct);
        }
        
        private static void ExampleReadWriteMessagesWithTimeout(CancellationToken ct)
        {
            // Example for simple raw message read (lowest level possible)
            new ReadMessageWithTimeout().Start();

            // Example for simple raw message write (lowest level possible)
            new WriteMessage().Run(ct);
        }

        private static void ExampleReadWithoutConsumerGroup(CancellationToken ct)
        {
            // Example for simple raw message read (lowest level possible)
            new ReadMessage().Start(false, Offset.Beginning);

            // Example for simple raw message write (lowest level possible)
            new WriteMessage().Run(ct);
            //Thread.Sleep(-1);
        }
    }
}
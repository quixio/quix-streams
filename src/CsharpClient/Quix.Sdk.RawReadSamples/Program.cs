using System;
using System.Text;
using System.Threading;
using Confluent.Kafka;
using Quix.Sdk.Process.Configuration;
using Quix.Sdk.Streaming;
using Quix.Sdk.Streaming.Configuration;
using Quix.Sdk.Transport.Kafka;

namespace Quix.Sdk.RawReadSamples
{
    class Program
    {
        static void Main(string[] args)
        {

            (new Thread(() =>
            {
                TestReadMeta.Run();
            })).Start();

            (new Thread(() =>
            {
                TestWriteMeta.Run();
            })).Start();


/*
            (new Thread(() =>
            {
                TestReadKey.Run();
            })).Start();

            (new Thread(() =>
            {
                TestWriteKey.Run();
            })).Start();
*/
        }
    }
}

using System;

namespace QuixStreams.Kafka.Transport
{
    /// <summary>
    /// The producer is closed and operation is not valid
    /// </summary>
    public class ProducerClosedException : InvalidOperationException
    {
    }
}
using System;

namespace QuixStreams.Telemetry.Kafka
{
    /// <summary>The offset to use when there is no saved offset for the consumer group.</summary>
    public enum AutoOffsetReset
    {
        /// <summary>Latest, starts from newest message if there is no stored offset</summary>
        Latest = 0,
        /// <summary>Earliest, starts from the oldest message if there is no stored offset</summary>
        Earliest = 1,
        /// <summary>Error, throws exception if there is no stored offset</summary>
        Error = 2
    }

    /// <summary>
    /// Confluent Kafka types extension methods
    /// </summary>
    public static partial class ConfluentTypeConverter
    {
        /// <summary>
        /// Convert Quix AutoOffsetReset type into Confluent Kafka AutoOffsetReset type
        /// </summary>
        /// <param name="offsetReset">Quix AutoOffsetReset type</param>
        /// <returns>Confluent Kafka AutoOffsetReset type</returns>
        public static Confluent.Kafka.AutoOffsetReset ConvertToKafka(this AutoOffsetReset offsetReset)
        {
            switch (offsetReset)
            {
                case AutoOffsetReset.Latest:
                    return Confluent.Kafka.AutoOffsetReset.Latest;
                case AutoOffsetReset.Earliest:
                    return Confluent.Kafka.AutoOffsetReset.Earliest;
                case AutoOffsetReset.Error:
                    return Confluent.Kafka.AutoOffsetReset.Error;
                default:
                    throw new ArgumentOutOfRangeException(nameof(offsetReset), offsetReset, null);
            }
        }
    }
}
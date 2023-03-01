using Quix.Streams.Telemetry.Kafka;

namespace Quix.Streams.Telemetry.Common.Test
{
    /// <summary>
    /// Kafka producer that uses a mocked Message broker for test purposes
    /// </summary>
    public class TestTelemetryKafkaProducer : TelemetryKafkaProducer
    {
        /// <summary>
        /// Initializes a new instance of <see cref="TestTelemetryKafkaProducer"/>
        /// </summary>
        /// <param name="testBroker">Mocked test broker instance of <see cref="TestBroker"/></param>
        /// <param name="streamId">Writing stream Id</param>
        public TestTelemetryKafkaProducer(TestBroker testBroker, string streamId = null)
            :base(testBroker.Producer, null, streamId)
        {
        }
    }
}

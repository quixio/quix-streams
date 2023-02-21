using Quix.Sdk.Process;
using Quix.Sdk.Process.Kafka;

namespace Quix.Sdk.Process.Common.Test
{
    /// <summary>
    /// Kafka Writer that uses a mocked Message broker for test purposes
    /// </summary>
    public class TestKafkaWriter : KafkaWriter
    {
        /// <summary>
        /// Initializes a new instance of <see cref="TestKafkaWriter"/>
        /// </summary>
        /// <param name="testBroker">Mocked test broker instance of <see cref="TestBroker"/></param>
        /// <param name="streamId">Writing stream Id</param>
        public TestKafkaWriter(TestBroker testBroker, string streamId = null)
            :base(testBroker.Producer, streamId)
        {
        }
    }
}

using QuixStreams.Kafka.Transport.SerDes.Codecs;
using QuixStreams.Kafka.Transport.SerDes.Codecs.DefaultCodecs;

namespace QuixStreams.Kafka.Transport.Tests.Helpers
{
    public class ModelFactory
    {
        private static TestBroker Producer = new TestBroker();
        
        public static KafkaMessage CreateKafkaMessage(string key, object value)
        {
            CodecRegistry.RegisterCodec(new ModelKey(typeof(object)), DefaultJsonCodec.Instance);
            KafkaMessage result = null;
            Producer.OnMessageReceived = async message => result = message;  
            var tProducer = new KafkaTransportProducer(Producer);
            tProducer.Publish(new TransportPackage<object>(key, value));
            return result;
        }
        
        public static KafkaMessage CreateKafkaMessage(TransportPackage package)
        {
            CodecRegistry.RegisterCodec(new ModelKey(package.Type), DefaultJsonCodec.Instance);
            KafkaMessage result = null;
            Producer.OnMessageReceived = async message => result = message;  
            var tProducer = new KafkaTransportProducer(Producer);
            tProducer.Publish(package);
            return result;
        }
        
        public static TransportPackage ConvertToReceivedPackage(TransportPackage package)
        {
            CodecRegistry.RegisterCodec(new ModelKey(package.Type), DefaultJsonCodec.Instance);
            KafkaMessage result = null;
            Producer.OnMessageReceived = async message => result = message;  
            var tProducer = new KafkaTransportProducer(Producer);
            tProducer.Publish(package); 
            return new TransportPackage(package.Type, package.Key, package.Value, result);
        }
    }
}
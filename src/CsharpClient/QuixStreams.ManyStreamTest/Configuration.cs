using Microsoft.Extensions.Configuration;
using QuixStreams.Streaming.Configuration;

namespace QuixStreams.ManyStreamTest
{
    public class Configuration
    {
        public static KafkaConfiguration Config;

        static Configuration()
        {
            var builder = new ConfigurationBuilder();
            builder.AddJsonFile("appsettings.json", optional: false);
            var appConfig = builder.Build();
            
            Config = new KafkaConfiguration();
            appConfig.Bind("KafkaConfiguration", Config);
        }
    }

    public class KafkaConfiguration
    {
        public string BrokerList { get; set; }
        public string Topic { get; set; }
        public string ConsumerId { get; set; }
        public SecurityOptions Security{ get; set; }
    }
}
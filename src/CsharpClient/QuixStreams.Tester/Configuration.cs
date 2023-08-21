using System;
using Microsoft.Extensions.Configuration;
using QuixStreams.Streaming.Configuration;
using Enum = System.Enum;

namespace QuixStreams.Tester
{
    public class Configuration
    {
        public static KafkaConfiguration Config;

        public static ClientRunMode Mode;

        public static ProducerConfig ProducerConfig;

        static Configuration()
        {
            var builder = new ConfigurationBuilder();
            builder.AddJsonFile("appsettings.json", optional: false);
            var appConfig = builder.Build();
            
            Config = new KafkaConfiguration();
            appConfig.Bind("KafkaConfiguration", Config);

            if (!Enum.TryParse(appConfig.GetValue<string>("Mode"), true, out ClientRunMode mode))
            {
                throw new Exception("Failed to parse Mode. Must be either Producer or Consumer");
            }

            Configuration.Mode = mode;
            Console.WriteLine($"In {mode} setting.");
            
            ProducerConfig = new ProducerConfig();
            appConfig.Bind("ProducerConfig", ProducerConfig);
        }
    }

    public class ProducerConfig
    {
        public bool Timeseries { get; set; }
        
        public bool Event { get; set; }
    }

    public enum ClientRunMode
    {
        Producer,
        Consumer
    }

    public class KafkaConfiguration
    {
        public string BrokerList { get; set; }
        public string Topic { get; set; }
        public string ConsumerGroup { get; set; }
        public SecurityOptions Security{ get; set; }
    }
}
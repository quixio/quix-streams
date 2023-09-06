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
        
        public static ConsumerConfig ConsumerConfig;


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
            
            
            ConsumerConfig = new ConsumerConfig();
            appConfig.Bind("ConsumerConfig", ConsumerConfig);
        }
    }
    
    public class ConsumerConfig
    {
        public bool PrintAverage { get; set; }
        
        public bool PrintEvents { get; set; }
        
        public bool PrintTimeseries { get; set; }
        
        public bool PrintStreams { get; set; }

    }

    public class ProducerConfig
    {
        public int NumberOfStreams { get; set; }
        
        public bool TimeseriesEnabled => TimeseriesRate > 0;

        public bool EventsEnabled => EventRate > 0;
        

        public double EventRate { get; set; }
        
        public double TimeseriesRate { get; set; }
        
        public int RowPerTimeseries { get; set; }

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
﻿using Microsoft.Extensions.Configuration;
using QuixStreams.Streaming.Configuration;

namespace QuixStreams.Streaming.Samples
{
    public class Configuration
    {
        public static KafkaConfiguration Config;

        public static QuixStreamingClientConfig QuixStreamingClientConfig;

        static Configuration()
        {
            var builder = new ConfigurationBuilder();
            builder.AddJsonFile("appsettings.json", optional: false);
            var appConfig = builder.Build();
            
            Config = new KafkaConfiguration();
            appConfig.Bind("KafkaConfiguration", Config);

            QuixStreamingClientConfig = new QuixStreamingClientConfig();
            appConfig.Bind("QuixStreamingClientConfig", QuixStreamingClientConfig);
        }
    }

    public class QuixStreamingClientConfig
    {
        public string Token { get; set; }
        public string PortalApi { get; set; }
    }

    public class KafkaConfiguration
    {
        public string BrokerList { get; set; }
        public string Topic { get; set; }
        public string ConsumerId { get; set; }
        public SecurityOptions Security{ get; set; }
    }
}
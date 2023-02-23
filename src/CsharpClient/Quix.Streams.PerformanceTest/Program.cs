using System;
using System.Threading;
using Quix.Streams.Streaming.Configuration;
    
namespace WriteHelloWorld
{
    class Program
    {
        /// <summary>
        /// Main will be invoked when you run the application
        /// </summary>
        static void Main()
        {
            // Create a client which holds generic details for creating input and output topics
            var client = new Quix.Streams.Streaming.QuixStreamingClient();
    
            using var producer = client.CreateRawTopicProducer(TOPIC_ID);
    
            var data = new byte[]{1,3,5,7,1,43};
    
            //Publish value with KEY to kafka
            producer.Publish(new Streaming.Raw.RawMessage(
                MESSAGE_KEY,
                data
            ));
    
            //Publish value withhout key into kafka
            producer.Publish(new Streaming.Raw.RawMessage(
                data
            ));
        }
    }
}
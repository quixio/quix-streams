namespace QuixStreams.Streaming.Models
{
    public class TopicConsumerPartition
    {
        public string ConsumerGroup { get; }
        public string TopicName { get; }
        public int Partition { get; }
        
        public TopicConsumerPartition(string consumerGroup, string topicName, int partition)
        {
            this.ConsumerGroup = consumerGroup;
            this.TopicName = topicName;
            this.Partition = partition;
        }
    }
}
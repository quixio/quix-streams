using System;
using System.Collections.Generic;
using Quix.Sdk.Process.Models;

namespace Quix.Sdk.Streaming.Models.StreamReader
{
    /// <summary>
    /// Properties and Metadata of the stream.
    /// All the changes of these properties are populated to this class automatically
    /// </summary>
    public class StreamPropertiesReader : IDisposable
    {
        private readonly IInputTopic topic;
        private readonly IStreamReaderInternal streamReader;

        /// <summary>
        /// Initializes a new instance of <see cref="StreamPropertiesReader"/>
        /// </summary>
        /// <param name="topic">The topic the stream to what this reader belongs to</param>
        /// <param name="streamReader">Stream reader owner</param>
        internal StreamPropertiesReader(IInputTopic topic, IStreamReaderInternal streamReader)
        {
            this.topic = topic;
            this.streamReader = streamReader;

            this.Metadata = new Dictionary<string, string>();
            this.Parents = new List<string>();

            this.streamReader.OnStreamPropertiesChanged += OnStreamPropertiesChangedEventHandler;

        }

        private void OnStreamPropertiesChangedEventHandler(IStreamReader sender, StreamProperties streamProperties)
        {
            this.Name = streamProperties.Name;
            this.Location = streamProperties.Location;
            this.TimeOfRecording = streamProperties.TimeOfRecording;
            this.Metadata = streamProperties.Metadata;
            this.Parents = streamProperties.Parents;

            this.OnChanged?.Invoke(this.streamReader, new StreamPropertiesChangedEventArgs(this.topic, sender));
        }

        /// <summary>
        /// Raised when the stream properties have changed
        /// Sender is the stream the properties changed for
        /// </summary>
        public event EventHandler<StreamPropertiesChangedEventArgs> OnChanged;

        /// <summary>
        /// Gets the name of the stream
        /// </summary>
        public string Name { get; private set; }

        /// <summary>
        /// Gets the location of the stream
        /// </summary>
        public string Location { get; private set; }

        /// <summary>
        /// Gets the datetime of the recording
        /// </summary>
        public DateTime? TimeOfRecording { get; private set; }

        /// <summary>
        /// Gets the metadata of the stream
        /// </summary>
        public Dictionary<string, string> Metadata { get; private set; }

        /// <summary>
        /// List of Stream Ids of the Parent streams
        /// </summary>
        public List<string> Parents { get; private set; }

        /// <inheritdoc/>
        public void Dispose()
        {
            this.streamReader.OnStreamPropertiesChanged -= OnStreamPropertiesChangedEventHandler;
        }
    }

    public class StreamPropertiesChangedEventArgs
    {
        public StreamPropertiesChangedEventArgs(IInputTopic topic, IStreamReader reader)
        {
            this.Topic = topic;
            this.Stream = reader;
        }

        public IInputTopic Topic { get; }
        public IStreamReader Stream { get; }
    }
}

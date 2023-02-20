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
        private readonly Streaming.StreamReader streamReader;

        /// <summary>
        /// Initializes a new instance of <see cref="StreamPropertiesReader"/>
        /// </summary>
        /// <param name="streamReader">Stream reader owner</param>
        internal StreamPropertiesReader(Streaming.StreamReader streamReader)
        {
            this.streamReader = streamReader;

            this.Metadata = new Dictionary<string, string>();
            this.Parents = new List<string>();

            this.streamReader.OnStreamPropertiesChanged += OnStreamReaderOnOnStreamPropertiesChanged;

        }

        private void OnStreamReaderOnOnStreamPropertiesChanged(IStreamReaderInternal sender, StreamProperties streamProperties)
        {
            this.Name = streamProperties.Name;
            this.Location = streamProperties.Location;
            this.TimeOfRecording = streamProperties.TimeOfRecording;
            this.Metadata = streamProperties.Metadata;
            this.Parents = streamProperties.Parents;

            this.OnChanged?.Invoke(this.streamReader, EventArgs.Empty);
        }

        /// <summary>
        /// Raised when the stream properties have changed
        /// Sender is the stream the properties changed for
        /// </summary>
        public event EventHandler OnChanged;

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
            this.streamReader.OnStreamPropertiesChanged -= OnStreamReaderOnOnStreamPropertiesChanged;
        }
    }
}

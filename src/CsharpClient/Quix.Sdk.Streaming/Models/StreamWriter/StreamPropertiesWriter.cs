using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Threading;
using Microsoft.Extensions.Logging;
using Quix.Sdk.Process.Models;

namespace Quix.Sdk.Streaming.Models.StreamWriter
{
    /// <summary>
    /// Properties and Metadata of the stream.
    /// All the changes of these properties will be streamed and persisted to the platform.
    /// </summary>
    public class StreamPropertiesWriter : IDisposable
    {
        private readonly IStreamWriterInternal streamWriter;
        private string name;
        private string location;
        private DateTime? timeOfRecording;

        private readonly Timer flushTimer;
        private bool timerEnabled = false; // Here because every now and then reseting its due time to never doesn't work
        private const int propertyChangedFlushInterval = 20;
        private int lastHash = 0;
        private readonly object flushLock = new object();
        private bool isDisposed = false;

        private long lastHeartbeatRebroadcastTime = 0;  // in milliseconds
        private int heartbeatRebroadcastFlushInterval = 30*1000;
        private ILogger<StreamParametersWriter> logger = Logging.CreateLogger<StreamParametersWriter>();

        /// <summary>
        /// Automatic flush interval of the properties metadata into the channel [ in milliseconds ]
        /// </summary>
        public int FlushInterval
        {
            get
            {
                if (isDisposed)
                {
                    throw new ObjectDisposedException(nameof(StreamPropertiesWriter));
                }
                return this.heartbeatRebroadcastFlushInterval;
            }
            set
            {
                if (isDisposed)
                {
                    throw new ObjectDisposedException(nameof(StreamPropertiesWriter));
                }

                if (value <= 0)
                {
                    throw new ArgumentOutOfRangeException(nameof(value),"Flush interval must be greater than 0");
                }
                this.heartbeatRebroadcastFlushInterval = value;
            }
        }

        /// <summary>
        /// Initializes a new instance of <see cref="StreamPropertiesWriter"/>
        /// </summary>
        /// <param name="streamWriter">Stream writer owner</param>
        internal StreamPropertiesWriter(IStreamWriterInternal streamWriter)
        {
            this.streamWriter = streamWriter;
            streamWriter.OnBeforeSend += new Action<Type>((type =>
            {
                // To handle ClosePacket
                if (isDisposed)
                    return;

                // To avoid circular infinite recursion
                if (type != typeof(StreamProperties))
                {
                    this.CheckForHeartbeatFlush();
                }
            }));

            this.Metadata = new ObservableDictionary<string, string>();
            this.Metadata.CollectionChanged += (sender, e) =>
            {
                this.PushWrite();
            };
            this.Parents = new ObservableCollection<string>();
            this.Parents.CollectionChanged += (sender, e) =>
            {
                this.PushWrite();
            };

            // Timer for delayed writes
            flushTimer = new Timer((state) =>
            {
                if (!timerEnabled) return;
                this.Flush();
            }, null, Timeout.Infinite, Timeout.Infinite);
        }

        /// <summary>
        /// Name of the stream.
        /// </summary>
        public string Name
        {
            get => name; set
            {
                if (isDisposed)
                {
                    throw new ObjectDisposedException(nameof(StreamPropertiesWriter));
                }
                name = value;
                this.PushWrite();
            }
        }

        /// <summary>
        /// Specify location of the stream in data catalogue. 
        /// For example: /cars/ai/carA/.
        /// </summary>
        public string Location
        {
            get => location; set
            {
                if (isDisposed)
                {
                    throw new ObjectDisposedException(nameof(StreamPropertiesWriter));
                }
                location = value;
                this.PushWrite();
            }
        }

        /// <summary>
        /// Date Time of recording of the stream. Commonly set to Datetime.UtcNow.
        /// </summary>
        public DateTime? TimeOfRecording
        {
            get => timeOfRecording; set
            {
                if (isDisposed)
                {
                    throw new ObjectDisposedException(nameof(StreamPropertiesWriter));
                }
                timeOfRecording = value;
                this.PushWrite();
            }
        }

        /// <summary>
        /// Metadata of the stream.
        /// </summary>
        private void CheckForHeartbeatFlush()
        {
            long curms = DateTimeOffset.Now.ToUnixTimeMilliseconds();
            if (curms > this.lastHeartbeatRebroadcastTime + this.FlushInterval)
            {
                this.Flush(false, false);
            }
        }

        /// <summary>
        /// Metadata of the stream.
        /// </summary>
        public ObservableDictionary<string, string> Metadata { get; }

        /// <summary>
        /// List of Stream Ids of the Parent streams
        /// </summary>
        public ObservableCollection<string> Parents { get; }

        /// <summary>
        /// Adds a parent stream.
        /// </summary>
        /// <param name="parentStreamId">Stream Id of the parent</param>
        public void AddParent(string parentStreamId)
        {
            if (isDisposed)
            {
                throw new ObjectDisposedException(nameof(StreamPropertiesWriter));
            }
            // TODO REMOVE this or add extra logic to validate it
            this.Parents.Add(parentStreamId);
        }

        /// <summary>
        /// Removes a parent stream
        /// </summary>
        /// <param name="parentStreamId">Stream Id of the parent</param>
        public void RemoveParent(string parentStreamId)
        {
            if (isDisposed)
            {
                throw new ObjectDisposedException(nameof(StreamPropertiesWriter));
            }
            // TODO REMOVE this or add extra logic to validate it
            this.Parents.Remove(parentStreamId);
        }

        /// <summary>
        /// Immediately writes the properties yet to be sent instead of waiting for the flush timer (20ms)
        /// </summary>
        public void Flush()
        {
            this.Flush(false);
        }

        private void Flush(bool force, bool flushOnlyOnChange = true)
        {
            if (!force && isDisposed)
            {
                throw new ObjectDisposedException(nameof(StreamPropertiesWriter));
            }


            timerEnabled = false;
            this.flushTimer.Change(Timeout.Infinite, Timeout.Infinite);
            lock (flushLock)
            {
                
                var streamProperties = new StreamProperties
                {
                    Location = this.location,
                    Name = this.name,
                    Parents = GetParents(),
                    Metadata = GetMetadata(),
                    TimeOfRecording = this.timeOfRecording
                };

                var hash = streamProperties.GetHashCode();
                if (flushOnlyOnChange && hash == lastHash)
                {
                    return;
                }

                this.lastHash = hash;
                this.lastHeartbeatRebroadcastTime = DateTimeOffset.Now.ToUnixTimeMilliseconds();

                this.streamWriter.Write(streamProperties);
            }


            List<string> GetParents()
            {
                // This is the easiest way to avoid collection modification exception. Any other solution would require me to
                // have a synchronizing object within the observable list or reinvent an observable concurrent bag. Due to the number of times
                // this can happen (on every flush) that would end up being more expensive than this brute-force
                while (true)
                {
                    try
                    {
                        return this.Parents.ToList();
                    }
                    catch (System.ArgumentException ex)
                    {
                        this.logger.LogTrace(ex, "Exception while trying to get stream metadata");
                    }
                    catch (System.InvalidOperationException ex)
                    {
                        this.logger.LogTrace(ex, "Exception while trying to get stream parents");
                    }
                }
            }
            
            Dictionary<string, string> GetMetadata()
            {
                // This is the easiest way to avoid collection modification exception. Any other solution would require me to
                // have a synchronizing object within the observable dictionary or reinvent an observable concurrent dictionary. Due to the number of times
                // this can happen (on every flush) that would end up being more expensive than this brute-force
                while (true)
                {
                    try
                    {
                        return this.Metadata.ToDictionary(kv => kv.Key, kv => kv.Value);
                    }
                    catch (System.ArgumentException ex)
                    {
                        this.logger.LogTrace(ex, "Exception while trying to get stream metadata");
                    }
                    catch (System.InvalidOperationException ex)
                    {
                        this.logger.LogTrace(ex, "Exception while trying to get stream metadata");
                    }
                }
            }
        }

        private void PushWrite()
        {
            timerEnabled = true;
            this.flushTimer.Change(propertyChangedFlushInterval, Timeout.Infinite);
        }

        /// <summary>
        /// Flushes internal buffers and disposes
        /// </summary>
        public void Dispose()
        {
            if (this.isDisposed) return;
            this.isDisposed = true;
            this.Flush(true);
            flushTimer?.Dispose();
        }
    }
}

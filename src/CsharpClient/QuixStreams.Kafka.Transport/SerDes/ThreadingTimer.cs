using System;
using System.Threading;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace QuixStreams.Kafka.Transport.SerDes
{
    /// <summary>
    /// Helper class for easy handling of <see cref="System.Threading.Timer"/>
    /// </summary>
    internal class ThreadingTimer : IDisposable
    {
        private readonly Action callback;
        private readonly int timespanInMs;
        private bool isEnabled; // Here because every now and then resetting its due time to never doesn't work
        private readonly Timer timer;
        private bool disposed;
        private readonly ILogger logger;

        public ThreadingTimer(Action callback, TimeSpan timeSpan, ILogger logger = null) : this(callback, (int)Math.Round(timeSpan.TotalMilliseconds), logger)
        {
        }
        
        public ThreadingTimer(Action callback, int timespanInMs, ILogger logger = null)
        {
            this.logger = logger ?? NullLogger.Instance;
            this.callback = callback;
            this.timespanInMs = timespanInMs;
            isEnabled = false;
            timer = new Timer(TimerCallback, null, Timeout.Infinite, Timeout.Infinite); // Create timer
        }

        private void TimerCallback(object state)
        {
            if (!isEnabled) return;
            try
            {
                callback();
            }
            catch(Exception ex)
            {
                // there is nothing this thread can do about this
                logger.LogError(ex, "Exception in timer callback");
            }

            timer.Change(timespanInMs, Timeout.Infinite);
        }

        public void Start()
        {
            isEnabled = true;
            timer.Change(timespanInMs, Timeout.Infinite); // Reset / Enable timer
        }

        public void Stop()
        {
            isEnabled = false;
            timer.Change(Timeout.Infinite, Timeout.Infinite); // Disable flush timer
        }

        public void Restart()
        {
            Stop();
            Start();
        }

        public void Dispose()
        {
            if (this.disposed) return;
            this.disposed = true;
            Stop();
            timer?.Dispose();
        }
    }
}
using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace QuixStreams.State.Storage.FileStorage.LocalFileStorage
{
    /// <summary>
    /// The directory storage containing the file storage for the single process access purposes
    /// The locking is implemented via the in-memory mutex
    /// </summary>
    public sealed class LocalFileStorage : BaseFileStorage
    {
        private int cnt = 0;

        /// <summary>
        /// Lock structure
        /// </summary>
        private class CountedLock
        {
            /// <summary>
            /// Number of the locks waiting in the line
            /// </summary>
            public int WaitingNo = 0;

            /// <summary>
            /// Lock object
            /// </summary>
            public AsyncReaderWriterLock LockObj = new AsyncReaderWriterLock();
        }

        /// <summary>
        /// Set of the locks used for the internal purposes
        /// </summary>
        private readonly ConcurrentDictionary<string, CountedLock> internalLocks = new ConcurrentDictionary<string, CountedLock>();

        /// <summary>
        /// Instantiates a new instance of <see cref="LocalFileStorage"/>
        /// </summary>
        /// <param name="storageDirectory">The directory for storing the states</param>
        /// <param name="autoCreateDir">Whether the directory should be automatically created if does not exist already</param>
        public LocalFileStorage(string storageDirectory = null, bool autoCreateDir = true) : base(storageDirectory, autoCreateDir)
        {
        }

        /// <inheritdoc />
        protected override void AssertKey(string key)
        {
        }

        /// <inheritdoc />
        protected override async Task<IDisposable> LockInternalKey(string key = "", LockType type = LockType.Writer)
        {
            var id = Interlocked.Increment(ref this.cnt);
            CountedLock lockObj;
            lock (internalLocks)
            {
                if ( !internalLocks.TryGetValue(key, out lockObj) )
                {
                    lockObj = internalLocks[key] = new CountedLock();
                }
                lockObj.WaitingNo++;
            }

            switch (type)
            {
                case LockType.Reader:
                    await lockObj.LockObj.AcquireReaderLock();
                    break;
                case LockType.Writer:
                    await lockObj.LockObj.AcquireWriterLock();
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(type), "Lock type is invalid. Only Reader or Writer lock types are supported.");
            }

            var types = type == LockType.Reader ? "Reader" : "Writer";
            return new MemoryLock(() => {
                switch (type)
                {
                    case LockType.Reader:
                        lockObj.LockObj.ReleaseReaderLock();
                        break;
                    case LockType.Writer:
                        lockObj.LockObj.ReleaseWriterLock();
                        break;
                    default:
                        throw new ArgumentOutOfRangeException(nameof(type), "Lock type is invalid. Only Reader or Writer lock types are supported.");
                }
                lock (internalLocks)
                {
                    if (--lockObj.WaitingNo <= 0)
                    {
                        internalLocks.TryRemove(key, out var _);
                    }
                }
            });
        }

    }
}

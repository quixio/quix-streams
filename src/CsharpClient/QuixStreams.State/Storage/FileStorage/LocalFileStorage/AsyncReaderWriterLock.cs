using System;
using System.Threading;
using System.Threading.Tasks;

namespace QuixStreams.State.Storage.FileStorage.LocalFileStorage
{
        internal sealed class AsyncReaderWriterLock : IDisposable
        {
            private readonly SemaphoreSlim readSemaphore  = new SemaphoreSlim(1, 1);
            private readonly SemaphoreSlim writeSemaphore = new SemaphoreSlim(1, 1);
            private int readerCount;

            public async Task AcquireWriterLock(CancellationToken token = default)
            {
                await writeSemaphore.WaitAsync(token).ConfigureAwait(false);
                await SafeAcquireReadSemaphore(token).ConfigureAwait(false);
            }

            public void ReleaseWriterLock()
            {
                readSemaphore.Release();
                writeSemaphore.Release();
            }

            public async Task AcquireReaderLock(CancellationToken token = default)
            {
                await writeSemaphore.WaitAsync(token).ConfigureAwait(false);

                if (Interlocked.Increment(ref readerCount) == 1)
                {
                    try
                    {
                        await SafeAcquireReadSemaphore(token).ConfigureAwait(false);
                    }
                    catch
                    {
                        Interlocked.Decrement(ref readerCount);

                        throw;
                    }
                }

                writeSemaphore.Release();
            }

            public void ReleaseReaderLock()
            {
                if (Interlocked.Decrement(ref readerCount) == 0)
                {
                    readSemaphore.Release();
                }
            }

            private async Task SafeAcquireReadSemaphore(CancellationToken token)
            {
                try
                {
                    await readSemaphore.WaitAsync(token).ConfigureAwait(false);
                }
                catch
                {
                    writeSemaphore.Release();

                    throw;
                }
            }

            public void Dispose()
            {
                writeSemaphore.Dispose();
                readSemaphore.Dispose();
            }
        }
        
}
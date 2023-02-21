using System;

namespace Quix.Sdk.State.Storage.FileStorage.LocalFileStorage
{
    /// <summary>
    /// Implementation of the iDisposable 
    /// calls callback ( passed in the constructor ) on dispose
    /// </summary>

    internal class MemoryLock : IDisposable
    {
        public delegate void CallBack();

        protected CallBack onDispose;

        public MemoryLock(CallBack onDispose)
        {
            this.onDispose = onDispose;
        }

        public void Dispose()
        {
            this.onDispose();
        }
    }
}

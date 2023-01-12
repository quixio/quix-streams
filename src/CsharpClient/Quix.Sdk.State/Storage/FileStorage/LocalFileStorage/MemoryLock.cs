using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

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

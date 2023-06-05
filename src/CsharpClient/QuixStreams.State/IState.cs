using System;

namespace QuixStreams.State
{
    public interface IState
    {
        event EventHandler OnFlushing;
        event EventHandler OnFlushed;

        void Clear();
        void Flush();
        void Reset();
    }
}
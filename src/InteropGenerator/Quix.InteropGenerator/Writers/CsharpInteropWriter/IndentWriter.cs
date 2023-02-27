using System;
using Quix.InteropGenerator.Writers.Shared;

namespace Quix.InteropGenerator.Writers.CsharpInteropWriter;

internal class IndentWriter<T> : IDisposable where T: IIndentWriter, IContentWriter
{
    private readonly T contentWriter;
    private readonly Action<T> onIndentClosing;

    public IndentWriter(T contentWriter, Action<T> onIndentClosing = null)
    {
        this.contentWriter = contentWriter;
        this.onIndentClosing = onIndentClosing;
        contentWriter.Write("{");
        contentWriter.IncrementIndent();
    }

    public void Dispose()
    {
        if (onIndentClosing != null) onIndentClosing(this.contentWriter);
        contentWriter.DecrementIndent();
        contentWriter.Write("}");
    }
}
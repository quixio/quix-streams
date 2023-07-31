using System;
using Quix.InteropGenerator.Writers.Shared;

namespace Quix.InteropGenerator.Writers.CsharpInteropWriter;

internal class ExceptionHandlerWriter<T> : IDisposable where T: IIndentWriter, IContentWriter
{
    private readonly T contentWriter;
    private readonly Func<Type> getReturnType;
    private readonly Action<T> onExceptionCallback;
    private readonly Action<T> onFinallyCallback;

    public ExceptionHandlerWriter(T contentWriter, Func<Type> getReturnType, Action<T> onExceptionCallback = null, Action<T> onFinallyCallback = null)
    {
        this.contentWriter = contentWriter;
        this.getReturnType = getReturnType;
        this.onExceptionCallback = onExceptionCallback;
        this.onFinallyCallback = onFinallyCallback;

        contentWriter.Write("try");
        contentWriter.Write("{");
        contentWriter.IncrementIndent();
    }

    public void Dispose()
    {
        contentWriter.DecrementIndent();
        contentWriter.Write("}");
        contentWriter.Write("catch (Exception ex)");
        contentWriter.Write("{");
        contentWriter.IncrementIndent();
        if (onExceptionCallback != null)
        {
            onExceptionCallback(contentWriter);
        }
        contentWriter.Write("InteropUtils.RaiseException(ex);");
        if (getReturnType() != typeof(void)) contentWriter.Write("return default;");
        contentWriter.DecrementIndent();
        contentWriter.Write("}");
        if (onFinallyCallback == null) return;
        contentWriter.Write("finally");
        contentWriter.Write("{");
        contentWriter.IncrementIndent();
        onFinallyCallback(contentWriter);
        contentWriter.DecrementIndent();
        contentWriter.Write("}");
    }
}
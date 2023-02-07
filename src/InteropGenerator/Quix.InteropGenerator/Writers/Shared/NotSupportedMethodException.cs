using System;

namespace Quix.InteropGenerator.Writers.Shared;

public class NotSupportedMethodException : NotSupportedException
{
    public string EntryPoint { get; }

    public NotSupportedMethodException(string entryPoint)
    {
        EntryPoint = entryPoint;
    }
}
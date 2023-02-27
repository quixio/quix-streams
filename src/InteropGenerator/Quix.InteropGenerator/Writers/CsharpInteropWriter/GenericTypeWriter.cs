using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Quix.InteropGenerator.Writers.Shared;

namespace Quix.InteropGenerator.Writers.CsharpInteropWriter;

public class GenericTypeWriter : TypeWriter
{

    public GenericTypeWriter(Type type, List<string> extraUsings, TypeWrittenDetails typeWrittenDetails, List<Type> allowedTypes) : base(type, extraUsings, typeWrittenDetails, allowedTypes)
    {
        // WIP
    }

    public override async Task WriteContent(Func<string, Task> writeLineAction)
    {
       
    }
}
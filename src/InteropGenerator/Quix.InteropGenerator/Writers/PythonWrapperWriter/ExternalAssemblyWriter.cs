using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Quix.InteropGenerator.Writers.Shared;

namespace Quix.InteropGenerator.Writers.PythonWrapperWriter;

internal class ExternalAssemblyWriter : AssemblyWriter
{
    public ExternalAssemblyWriter(CsharpWrittenDetails writtenDetails, string basePath) : base(writtenDetails, basePath)
    {
    }

    public override async Task WriteContent(Dictionary<Type, string> typeLookups)
    {
        typeLookups ??= new Dictionary<Type, string>();
        await WriteTypes(typeLookups);
    }
}
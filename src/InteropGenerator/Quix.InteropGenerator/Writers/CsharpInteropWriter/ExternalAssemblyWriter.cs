using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading.Tasks;
using Quix.InteropGenerator.Writers.CsharpInteropWriter.Helpers;
using Quix.InteropGenerator.Writers.Shared;

namespace Quix.InteropGenerator.Writers.CsharpInteropWriter;

/// <summary>
/// Assembly writer creates the project, handles external references & iterates for types
/// </summary>
internal class ExternalAssemblyWriter : AssemblyWriter
{
    private readonly List<Type> assemblyTypesToWrite;
    private readonly List<Type> allowedTypeReferences;


    public ExternalAssemblyWriter(Assembly assembly, CsharpWrittenDetails writtenDetails, bool singleAssembly, string basePath,
        string configPath, string utilsCsProjPath, List<Type> assemblyAssemblyTypes, List<Type> allowedTypeReferences)
        : base(assembly, writtenDetails, singleAssembly, basePath, configPath, utilsCsProjPath)
    {
        this.assemblyTypesToWrite = assemblyAssemblyTypes;
        this.allowedTypeReferences = allowedTypeReferences;
    }

    public override async Task WriteContent()
    {
        if (!this.SingleAssembly) await AssemblyHelpers.CreateCsProjFromAssembly(this.BasePath, this.ConfigPath, this.Assembly, this.UtilsCsProjPath);
        await WriteTypes(this.assemblyTypesToWrite, allowedTypeReferences);
    }

    protected override string GetProjectName()
    {
        return AssemblyHelpers.GetAssemblyName(this.Assembly);
    }
}
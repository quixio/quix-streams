using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Quix.InteropGenerator.Writers.PythonWrapperWriter.Helpers;
using Quix.InteropGenerator.Writers.Shared;

namespace Quix.InteropGenerator.Writers.PythonWrapperWriter;

public class AssemblyWriter
{
    protected readonly CsharpWrittenDetails WrittenDetails;
    protected readonly string BasePath;
    protected readonly string AssemblyFolder;

    public AssemblyWriter(CsharpWrittenDetails writtenDetails, string basePath)
    {
        this.WrittenDetails = writtenDetails;
        this.BasePath = basePath; 
        this.AssemblyFolder = Path.Combine(basePath, AssemblyHelpers.GetInteropAssemblyName(this.WrittenDetails.Assembly));
    }

    public virtual async Task WriteContent(Dictionary<Type, string> typeLookups)
    {
        typeLookups ??= new Dictionary<Type, string>();
        
        // Write the external assemblies. They are requirements of this assembly, so should be done first in order to know all used type location
        await WriteExternalAssemblies(typeLookups);

        // Write this assembly after figuring out where each type will be. That is necessary in order to write all types with proper relative references
        UpdateTypeLookup(typeLookups);
        await WriteTypes(typeLookups);
    }

    private async Task WriteExternalAssemblies(Dictionary<Type, string> typeLookups)
    {
        // Add the external assemblies
        var writers = this.WrittenDetails.ExternalAssemblies.Select(x => new ExternalAssemblyWriter(x, this.BasePath)).ToList();
        
        // necessary to ensure we know where to find each type
        foreach (var externalAssemblyWriter in writers)
        {
            externalAssemblyWriter.UpdateTypeLookup(typeLookups);
        }
        
        foreach (var externalAssemblyWriter in writers)
        {
            await externalAssemblyWriter.WriteContent(typeLookups);
        }
    }

    /// <summary>
    /// Updates the type lookups so we know where each type can be found when writing the python code
    /// </summary>
    /// <param name="typeLookups"></param>
    private void UpdateTypeLookup(Dictionary<Type, string> typeLookups)
    {
        // Create a lookup, so we can pass it to typewriters
        foreach (var type in this.WrittenDetails.Enums)
        {
            var typePath = Utils.GetTypePath(type);
            var relativePath = typePath.Replace('.', Path.DirectorySeparatorChar) + ".py";
            if (Path.GetFileName(relativePath).Contains('+')) relativePath = relativePath.Replace("+", "");
            var path = Path.Combine(this.AssemblyFolder, relativePath);
            typeLookups.Add(type, path);
        }

        foreach (var typeDetails in this.WrittenDetails.TypeDetails)
        {
            var typePath = Utils.GetTypePath(typeDetails.Type);
            var relativePath = typePath + ".py";
            if (Path.GetFileName(relativePath).Contains('+')) relativePath = relativePath.Replace("+", "");
            var path = Path.Combine(this.AssemblyFolder, relativePath);
            typeLookups.Add(typeDetails.Type, path);
        }
        
        foreach (var type in this.WrittenDetails.ExtraEnums)
        {
            var relativePath = Path.Join("ExternalTypes", type.FullName.TrimStart('.').Replace('.', Path.DirectorySeparatorChar) + ".py");
            if (Path.GetFileName(relativePath).Contains('+')) relativePath = relativePath.Replace("+", "");
            var path = Path.Combine(this.AssemblyFolder, relativePath);
            typeLookups.Add(type, path);
        }
    }

    protected async Task WriteTypes(Dictionary<Type, string> typeLookups)
    {
        // do the actual typewriting
        foreach (var type in this.WrittenDetails.Enums)
        {
            var path = typeLookups[type];
            Directory.CreateDirectory(Path.GetDirectoryName(path));
            using var sw = new StreamWriter(File.OpenWrite(path));
            var writer = new EnumWriter(type);
            await writer.WriteContent(sw.WriteLineAsync);
        }
        
        foreach (var typeDetails in this.WrittenDetails.TypeDetails)
        {
            var path = typeLookups[typeDetails.Type];
            Directory.CreateDirectory(Path.GetDirectoryName(path));
            using var sw = new StreamWriter(File.OpenWrite(path));
            var writer = new TypeWriter(typeDetails, typeLookups);
            await writer.WriteContent(sw.WriteLineAsync);
        }
        
        foreach (var type in this.WrittenDetails.ExtraEnums)
        {
            var path = typeLookups[type];
            Directory.CreateDirectory(Path.GetDirectoryName(path));
            using var sw = new StreamWriter(File.OpenWrite(path));
            var writer = new EnumWriter(type);
            await writer.WriteContent(sw.WriteLineAsync);
        }
    }
}
using System;
using System.IO;
using System.Threading.Tasks;
using Quix.InteropGenerator.Writers.CsharpInteropWriter.Helpers;

namespace Quix.InteropGenerator.Writers.CsharpInteropWriter;

/// <summary>
/// Writer dealing with the helper project writing
/// </summary>
public class UtilsAssemblyWriter
{
    private readonly string basePath;
    private readonly bool singleAssembly;
    private readonly string projectName;
    private const string AssemblyName = "InteropHelpers";
    private readonly string InteropAssemblyName = AssemblyHelpers.GetInteropAssemblyName(AssemblyName);
    
    

    public UtilsAssemblyWriter(string basePath, bool singleAssembly)
    {
        this.projectName = singleAssembly ? AssemblyName : InteropAssemblyName;
        this.basePath = basePath;
        this.singleAssembly = singleAssembly;
        this.ProjectPath = singleAssembly ? string.Empty : AssemblyHelpers.GetCsProjPath(basePath, projectName);
    }

    public string ProjectPath { get; }

    public async Task WriteContent()
    {
        var extraPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "./Writers/CsharpInteropWriter/InteropHelpers");
        if (!singleAssembly) await AssemblyHelpers.CreateCsProj(basePath, null, projectName);
        foreach (var file in Directory.GetFiles(extraPath, "*.*", SearchOption.AllDirectories))
        {
            var content = await File.ReadAllTextAsync(file);
            var replaced = content.Replace("InteropHelpers.Interop", InteropAssemblyName);
            var path = Path.Combine(basePath, projectName, Path.GetRelativePath(extraPath, file)); 
            Directory.CreateDirectory(Path.GetDirectoryName(path));
            await File.WriteAllTextAsync(path, replaced);            
        }
    }
}
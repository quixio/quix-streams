using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace Quix.InteropGenerator.Writers.PythonWrapperWriter;

public class UtilsAssemblyWriter
{
    private readonly string assemblyFolder;

    public UtilsAssemblyWriter(string basePath)
    {
        this.assemblyFolder = Path.Combine(basePath, "InteropHelpers");
    }
    
    public async Task<Dictionary<Type, string>> WriteContent()
    {
        var typeLookups = new Dictionary<Type, string>();
        await WriteInteropUtils(typeLookups);
        return typeLookups;
    }
    
    private async Task WriteInteropUtils(Dictionary<Type, string> typeLookups)
    {
        var extraPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "./Writers/PythonWrapperWriter/InteropHelpers");
        foreach (var file in Directory.GetFiles(extraPath, "*.*", SearchOption.AllDirectories))
        {
            var content = await File.ReadAllTextAsync(file);
            var name = Path.GetFileName(file);
            var path = Path.Combine(assemblyFolder, Path.GetRelativePath(extraPath, file));
            Directory.CreateDirectory(Path.GetDirectoryName(path));
            await File.WriteAllTextAsync(path, content);

            var extraType = ExtraTypes.GetType(Path.ChangeExtension(name, "").TrimEnd('.'));
            if (extraType != null)
            {
                typeLookups[extraType] = path;
            }
        }
    }
}
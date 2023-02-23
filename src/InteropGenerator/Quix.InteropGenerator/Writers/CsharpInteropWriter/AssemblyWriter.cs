using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using InteropHelpers.Interop;
using InteropHelpers.Interop.ExternalTypes.System;
using Quix.InteropGenerator.Writers.CsharpInteropWriter.Helpers;
using Quix.InteropGenerator.Writers.Shared;

namespace Quix.InteropGenerator.Writers.CsharpInteropWriter;

/// <summary>
/// Assembly writer creates the project, handles external references & iterates for types
/// </summary>
public class AssemblyWriter
{
    protected readonly Assembly Assembly;
    protected readonly CsharpWrittenDetails WrittenDetails;
    protected readonly bool SingleAssembly;
    private readonly int maxAssemblyDepth;
    private readonly List<Regex> whitelist;
    private readonly int additionalWhitelistDepth;
    protected readonly string BasePath;
    protected readonly string ConfigPath;
    protected readonly string UtilsCsProjPath;

    private const bool IgnoreGenerics = true;

    public AssemblyWriter(Assembly assembly, CsharpWrittenDetails writtenDetails, bool singleAssembly, string basePath, string configPath, string utilsCsProjPath,
        int maxAssemblyDepth, List<Regex> whitelist, int additionalWhitelistDepth) : this(assembly, writtenDetails, singleAssembly,
        basePath, configPath, utilsCsProjPath)
    {
        this.maxAssemblyDepth = maxAssemblyDepth;
        this.whitelist = whitelist;
        this.additionalWhitelistDepth = additionalWhitelistDepth;
        this.WrittenDetails.Assembly = assembly;
        if (maxAssemblyDepth < 1) throw new ArgumentOutOfRangeException(nameof(maxAssemblyDepth), "Must be min 1");
    }

    protected AssemblyWriter(Assembly assembly, CsharpWrittenDetails writtenDetails, bool singleAssembly, string basePath, string configPath, string utilsCsProjPath)
    {
        this.BasePath = basePath;
        this.ConfigPath = configPath;
        this.UtilsCsProjPath = utilsCsProjPath;
        this.Assembly = assembly;
        this.WrittenDetails = writtenDetails;
        this.SingleAssembly = singleAssembly;
        this.WrittenDetails.Assembly = assembly;
    }

    public virtual async Task WriteContent()
    {
        await AssemblyHelpers.CreateCsProjFromAssembly(this.BasePath, this.ConfigPath, this.Assembly, this.UtilsCsProjPath);
        
        var typesToWrite = AssemblyHelpers.GetPublicAssemblyTypes(this.Assembly);
        var allowedTypes = AssemblyHelpers.GetReferencedTypes(typesToWrite, this.maxAssemblyDepth, this.whitelist, this.additionalWhitelistDepth);

        var assemblies = allowedTypes.Select(y => y.Assembly).Distinct().ToList();
        var explicitlyIncludedTypes = assemblies.SelectMany(y => AssemblyHelpers.GetExplicitlyIncludedTypes(this.ConfigPath, y)).ToList();
        var allowedTypesWithExplicit = allowedTypes.Union(explicitlyIncludedTypes).ToList();
        await WriteTypes(typesToWrite, allowedTypesWithExplicit);
        
        var ignoredTypes = new List<Type>()
        {
            typeof(string), // we don't need this, as we handle it directly
            typeof(bool), // we don't need this, as we handle it directly
            typeof(object), // not very useful to generate bindings for
            typeof(EventArgs), // valueless eventargs, not really useful on its own
            typeof(Array), // manual
            typeof(IEnumerable), // manual
            typeof(ICollection), // manual
        };

        // Find types that we are using, but the unmanaged code only refers to them as pointer or similar
        var referencedTypes = allowedTypesWithExplicit
            .Where(y => y.GUID != Guid.Empty) // These are things we don't want, like non-proper type 
            .Except(typesToWrite).Where(y =>
            {
                return !Utils.IsDelegate(y) &&
                       !Utils.UnmanagedTypes.Contains(y) &&
                       !ignoredTypes.Contains(y) &&
                       !Utils.IsException(y) && // not extremely useful in unmanaged context
                       !y.IsArray;
            }).ToList();
        
        await WriteExternalTypes(referencedTypes, allowedTypesWithExplicit);
    }

    protected virtual string GetProjectName()
    {
        return AssemblyHelpers.GetInteropAssemblyName(this.Assembly);
    }

    /// <summary>
    /// Write the types found in the assembly limited by the allowed types
    /// </summary>
    /// <param name="typesToWrite">The types to write</param>
    /// <param name="allowedTypes">The types that are allowed to be used in code</param>
    /// <returns></returns>
    protected async Task WriteTypes(List<Type> typesToWrite, List<Type> allowedTypes)
    {
        var projectName = GetProjectName();
        var interopHelperNamespace = typeof(InteropUtils).Namespace;
        var dictionaryInteropNamespace = typeof(DictionaryInterop).Namespace;
        foreach (var type in typesToWrite)
        {
            if (Utils.IsEnum(type))
            {
                this.WrittenDetails.Enums.Add(type);
                Console.WriteLine("Ignoring enum " + type);
                continue; // ignore enums, there is not much to do with them
            }

            if (IgnoreGenerics && type.IsGenericType)
            {
                // WIP ...
                Console.WriteLine("Ignoring Generic " + type);
                continue;
            }

            if (Utils.IsException(type))
            {
                Console.WriteLine("Ignoring exception " + type);
                continue;
            }

            
            var typePath = Utils.GetTypePath(type);
            var relativePath = Path.Join(projectName, typePath + ".cs").TrimStart('.');
            
            var path = Path.Combine(this.BasePath, relativePath);
            Directory.CreateDirectory(Path.GetDirectoryName(path));
            await using var sw = new StreamWriter(File.OpenWrite(path));
            var typeWrittenDetails = new TypeWrittenDetails();
            this.WrittenDetails.TypeDetails.Add(typeWrittenDetails);


            TypeWriter writer;
            if (type.IsGenericType)
            {
                writer = new GenericTypeWriter(type, new List<string>() { interopHelperNamespace, dictionaryInteropNamespace }, typeWrittenDetails, allowedTypes);
            }
            else
            {
                writer = new TypeWriter(type, new List<string>() { interopHelperNamespace, dictionaryInteropNamespace }, typeWrittenDetails, allowedTypes);
            }

            await writer.WriteContent(sw.WriteLineAsync);
        }
    }

    private async Task WriteExternalTypes(List<Type> externalTypes, List<Type> allowedTypes)
    {
        foreach (var typeGroup in externalTypes.GroupBy(y=> y.Assembly))
        {
            var externalAssembly = typeGroup.Key;
            var externalWrittenDetails = new CsharpWrittenDetails();
            var externalAssemblyTypes = typeGroup.ToList(); // these are the types from this external type we actively use
            var basePathToUse = this.SingleAssembly
                ? AssemblyHelpers.GetSingleAssemblyBasePath(this.BasePath, this.Assembly)
                : this.BasePath;
            var writer = new ExternalAssemblyWriter(externalAssembly, externalWrittenDetails, this.SingleAssembly, basePathToUse, this.ConfigPath, this.UtilsCsProjPath, externalAssemblyTypes, allowedTypes);
            await writer.WriteContent();
            this.WrittenDetails.ExternalAssemblies.Add(externalWrittenDetails);
        }
    }
}
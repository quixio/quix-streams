using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Xml;
using Microsoft.Extensions.Logging;

namespace Quix.InteropGenerator.Writers.CsharpInteropWriter.Helpers;

public class AssemblyHelpers
{
    private static ILogger logger = Logger.LoggerFactory.CreateLogger<AssemblyHelpers>();
    
    /// <summary>
    /// Returns the additional types referenced by the provided list within the limits in addition to the original types
    /// </summary>
    /// <param name="types">The types to search for references</param>
    /// <param name="maxDepth">The maximum assembly depth to dig into. More than 1 or 2 can introduce drastic increase in returned types</param>
    /// <param name="whitelist">The whitelist to match referenced assemblies against</param>
    /// <param name="additionalWhiteListDepth">The additional depth to use when the assembly matches whitelist. Can be useful to include more of a specific assembly but not others</param>
    /// <returns>All types found by the condition including the original types</returns>
    public static List<Type> GetReferencedTypes(List<Type> types, int maxDepth, List<Regex> whitelist,
        int additionalWhiteListDepth)
    {
        var additionalTypes = types.ToList();
        if (maxDepth == 1) return additionalTypes;
        var count = 2;
        var previousTypes = additionalTypes;
        while (true)
        {
            count++;
            var involvedTypes = previousTypes
                .Select(type => new { Type = type, infos = Utils.GetWrappableMethodInfos(type) })
                .SelectMany(y => y.infos.SelectMany(x => Utils.GetUsedTypes(y.Type, x)))
                .Union(previousTypes.SelectMany(y => y.GetFields().Select(z => z.FieldType)))
                .Distinct().SelectMany(Utils.GetUnderlyingTypes).Distinct().ToList();

            var newTypes = involvedTypes
                .Except(additionalTypes).ToList();

            var extraTypes = newTypes.Where(y => y.IsTypeDefinition).ToList();

            if (extraTypes.Count == 0) break; // not any new types, so no point going deeper
            additionalTypes.AddRange(extraTypes);

            if (count > maxDepth)
            {
                // check for whitelist regardless of depth
                if (whitelist.Count == 0) break;
                if (count > maxDepth + additionalWhiteListDepth) break;
                previousTypes = extraTypes.Where(y =>
                {
                    var assemblyName = y.Assembly.GetName().Name;
                    if (assemblyName == null) return false;
                    return whitelist.Any(y => y.IsMatch(assemblyName));
                }).ToList();
                if (previousTypes.Count == 0) break;
                continue;
            }

            previousTypes = extraTypes;
        }

        return additionalTypes;
    }

    /// <summary>
    /// Retrieves the public types found in the assembly. 
    /// </summary>
    /// <param name="assembly"></param>
    /// <returns></returns>
    public static List<Type> GetPublicAssemblyTypes(Assembly assembly)
    {
        return assembly.GetTypes().Where(y => (y.IsPublic || y.IsNestedPublic) && Utils.IsInteropSupported(y)).ToList();
    }

    /// <summary>
    /// Retrieves the interop namespace for the type
    /// </summary>
    /// <param name="type">The type to select the assembly from</param>
    /// <returns>The interop namespace</returns>
    public static string GetInteropNamespace(Type type)
    {
        return GetInteropAssemblyName(type.Namespace);
    }

    /// <summary>
    /// Retrieves the interop assembly name
    /// </summary>
    /// <param name="assembly">The assembly to retrieve the name for</param>
    /// <returns>The interop assembly name</returns>
    public static string GetInteropAssemblyName(Assembly assembly)
    {
        var name = assembly.GetName().Name;
        return GetInteropAssemblyName(name);
    }

    /// <summary>
    /// Retrieves the assembly name
    /// </summary>
    /// <param name="assembly">The assembly to get the name for</param>
    /// <returns>The assembly name</returns>
    public static string GetAssemblyName(Assembly assembly)
    {
        return assembly.GetName().Name;
    }

    /// <summary>
    /// Retrieves the interop assembly name
    /// </summary>
    /// <param name="originalAssemblyName">The original assembly name to convert</param>
    /// <returns>The interop assembly name</returns>
    public static string GetInteropAssemblyName(string originalAssemblyName)
    {
        if (string.IsNullOrWhiteSpace(originalAssemblyName)) return "Interop"; // ????
        return $"{originalAssemblyName}.Interop";
    }

    /// <summary>
    /// Returns the path for where dependencies should be output to when using the generator in single assembly mode
    /// </summary>
    /// <param name="basePath">the base path</param>
    /// <param name="assembly">The main assembly we're generating binding and dependencies for</param>
    /// <returns>The base path for dependencies</returns>
    public static string GetSingleAssemblyBasePath(string basePath, Assembly assembly)
    {
        var mainAssemblyName = AssemblyHelpers.GetAssemblyName(assembly);
        var mainInteropAssemblyName = AssemblyHelpers.GetInteropAssemblyName(mainAssemblyName);
        var mainCsProjPath = AssemblyHelpers.GetCsProjPath(basePath, mainInteropAssemblyName);

        // need the folder, so trim the csproj file from it
        var folder = Directory.GetParent(mainCsProjPath).FullName;

        return Path.Combine(folder, "ExternalTypes");
    }

    /// <summary>
    /// Returns the cs project path, but does not create it based on base path and project name
    /// </summary>
    /// <param name="basePath"></param>
    /// <param name="projectName"></param>
    /// <returns></returns>
    public static string GetCsProjPath(string basePath, string projectName)
    {
        var projectPath = Path.Join(basePath, projectName);
        var csprojFilePath = Path.Join(projectPath, $"{projectName}.csproj");
        return csprojFilePath;
    }

    public static async Task<string> CreateCsProj(string basePath, string configPath, string projectName)
    {
        var csprojFilePath = GetCsProjPath(basePath, projectName);
        var projectPath = Directory.GetParent(csprojFilePath).FullName;
        Directory.CreateDirectory(projectPath);

        var executingAssembly = Assembly.GetExecutingAssembly();

        using var stream =
            executingAssembly.GetManifestResourceStream(
                "Quix.InteropGenerator.Writers.CsharpInteropWriter.Resources.Template.csproj");
        XmlDocument projectXml = new XmlDocument();
        projectXml.Load(stream);

        // check for extra config
        if (!string.IsNullOrWhiteSpace(configPath))
        {
            var CsharpInteropWriterConfigPath = Path.Join(configPath, "CsharpInteropWriter");
            const string projectExtension = "projectextension.csproj";
            if (Directory.Exists(CsharpInteropWriterConfigPath))
            {
                var projectConfigPath = Path.Join(CsharpInteropWriterConfigPath, projectName);
                if (Directory.Exists(projectConfigPath))
                {
                    var files = Directory.GetFiles(projectConfigPath);
                    foreach (var filePath in files)
                    {
                        var fileName = Path.GetFileName(filePath);
                        if (fileName == projectExtension)
                        {

                            XmlDocument projectExtensionsXml = new XmlDocument();
                            projectExtensionsXml.Load(filePath);

                            var projectExtensionsRoot = projectExtensionsXml.FirstChild;
                            if (projectExtensionsRoot == null) continue; // erm, this shouldn't happen but to be safe

                            var projectXmlRoot = projectXml.FirstChild;
                            foreach (XmlNode node in projectExtensionsRoot.ChildNodes)
                            {
                                var newnode = projectXml.ImportNode(node, true);
                                projectXmlRoot.AppendChild(newnode);
                            }

                            continue;
                        }

                        var fileRelativePath = Path.GetRelativePath(projectConfigPath, filePath);
                        var newFilePath = Path.Join(projectPath, fileRelativePath);
                        File.Copy(filePath, newFilePath);
                    }
                }
            }
        }

        projectXml.Save(csprojFilePath);

        using var nugetStream =
            executingAssembly.GetManifestResourceStream(
                "Quix.InteropGenerator.Writers.CsharpInteropWriter.Resources.nuget.config");
        using var fs = File.OpenWrite(Path.Join(basePath, projectName, "nuget.config"));
        await nugetStream.CopyToAsync(fs);
        return csprojFilePath;
    }


    /// <summary>
    /// Creates an interop csproj for the given assembly under the base path
    /// </summary>
    /// <param name="basePath"></param>
    /// <param name="configPath">The extra config path</param>
    /// <param name="assembly"></param>
    /// <param name="utilsCsProjPath">The path to the utils interop csproj</param>
    /// <returns>The absolute path to the csproj</returns>
    public static async Task<string> CreateCsProjFromAssembly(string basePath, string configPath, Assembly assembly,
        string utilsCsProjPath)
    {
        var interopNamespace = AssemblyHelpers.GetInteropAssemblyName(assembly);

        var projFile = await CreateCsProj(basePath, configPath, interopNamespace);
        XmlDocument projectXml = new XmlDocument();
        projectXml.Load(projFile);
        var projectXmlRoot = projectXml.FirstChild;

        var projectPath = Directory.GetParent(projFile).FullName;

        // find the csproj that goes with the DLL
        var dllPath = assembly.Location;
        var csprojDir = Path.GetDirectoryName(dllPath);
        if (csprojDir != null)
        {
            var csprojPath = Path.Combine(csprojDir, Path.GetFileName(dllPath).Replace(".dll", ".csproj"));
            bool csProjExists = false;
            while (true)
            {
                csProjExists = File.Exists(csprojPath);
                if (csProjExists) break;
                csprojDir = Path.GetDirectoryName(csprojDir);
                if (csprojDir == null) break;
                csprojPath = Path.Combine(csprojDir, Path.GetFileName(csprojPath));
            }

            if (csProjExists)
            {
                var depNode = projectXml.CreateNode(XmlNodeType.Element, "ItemGroup", null);
                projectXmlRoot.AppendChild(depNode);

                var projDone = projectXml.CreateNode(XmlNodeType.Element, "ProjectReference", null);
                var includeAttr = projectXml.CreateAttribute("Include");
                includeAttr.Value = Path.GetRelativePath(projectPath, csprojPath);
                projDone.Attributes.Append(includeAttr);
                depNode.AppendChild(projDone);
            }
        }

        if (!string.IsNullOrWhiteSpace(utilsCsProjPath))
        {
            var depNode = projectXml.CreateNode(XmlNodeType.Element, "ItemGroup", null);
            projectXmlRoot.AppendChild(depNode);

            var projDone = projectXml.CreateNode(XmlNodeType.Element, "ProjectReference", null);
            var includeAttr = projectXml.CreateAttribute("Include");
            includeAttr.Value = Path.GetRelativePath(projectPath, utilsCsProjPath);
            projDone.Attributes.Append(includeAttr);
            depNode.AppendChild(projDone);
        }


        projectXml.Save(projFile);

        return projFile;
    }

    /// <summary>
    /// Retrieves the types defined within configuration
    /// </summary>
    /// <param name="configPath">The extra config path</param>
    /// <param name="assembly">The assembly to get the types for</param>
    /// <returns>The types to be included in assembly explicitly, even if not referenced in other code</returns>
    public static List<Type> GetExplicitlyIncludedTypes(string configPath, Assembly assembly)
    {
        var projectName = AssemblyHelpers.GetInteropAssemblyName(assembly);

        // check for extra config
        if (string.IsNullOrWhiteSpace(configPath))
        {
            return new List<Type>();
        }

        var CsharpInteropWriterConfigPath = Path.Join(configPath, "CsharpInteropWriter");
        const string rdFile = "rd.xml";
        var projectConfigPath = Path.Join(CsharpInteropWriterConfigPath, projectName);
        var rdPath = Path.Join(projectConfigPath, rdFile);

        if (!File.Exists(rdPath))
        {
            return new List<Type>();
        }


        var extraTypes = new List<Type>();

        XmlDocument projectExtensionsXml = new XmlDocument();
        projectExtensionsXml.Load(rdPath);

        var directivesRoot = projectExtensionsXml.FirstChild;
        if (directivesRoot == null) return extraTypes;

        
        var applicationRoot = directivesRoot.FirstChild;
        if (applicationRoot == null) return extraTypes;


        var assemblies = new List<Assembly>() { assembly };
        AddReferencedAssemblies(assembly, assemblies);
        
        foreach (XmlNode assemblyNode in applicationRoot.ChildNodes)
        {
            if (assemblyNode.Name != "Assembly") continue;
            var assemblyName = assemblyNode.Attributes["Name"].Value;
            var nodeAssembly = assemblies.FirstOrDefault(y=> y.GetName().Name == assemblyName);
            if (nodeAssembly == null)
            {
                if (TryAddAdditionalAssembly(assemblyName, assemblies)) nodeAssembly = assemblies.FirstOrDefault(y=> y.GetName().Name == assemblyName);
                if (nodeAssembly == null) throw new Exception($"Unable to find assembly {assemblyName}. It is not referenced by {assembly.FullName}");
            }
            foreach (XmlNode typeNode in assemblyNode.ChildNodes)
            {
                if (typeNode.Name != "Type") continue;
                var originalTypeName = typeNode.Attributes["Name"]?.Value;
                if (string.IsNullOrWhiteSpace(originalTypeName)) continue;
                var typeName = originalTypeName.Trim();

                // check if it is sth we can deal with now
                if (typeName.EndsWith("[]")) typeName = typeName.Substring(0, typeName.Length - 2);
                if (typeName.Contains("[]")) {
                    logger.LogInformation("Ignoring {0} for explicit type include, because it contains []", originalTypeName);
                    continue;
                }
                if (typeName.Contains('`')) {
                    logger.LogInformation("Ignoring {0} for explicit type include, not yet supporting generics (and likely won't)", originalTypeName);
                    continue;
                }

                try
                {
                    var type = nodeAssembly.GetType(typeName);
                    if (type.IsGenericType)
                    {
                        logger.LogInformation("Ignoring {0} for explicit type include, not yet supporting generics (and likely won't)", originalTypeName);
                        continue;
                    }
                    extraTypes.Add(type);
                    logger.LogInformation("Including {0} due to explicit type inclusion", originalTypeName);
                }
                catch (Exception ex)
                {
                    logger.LogError("Failed to load type {0}, could not find it in assembly {1}", originalTypeName, assembly.GetName());
                }
            }
        }

        return extraTypes;
    }


    private static bool TryAddAdditionalAssembly(string assemblyName, List<Assembly> assemblies)
    {
        var locations = assemblies.Select(y => y.Location)
            .Where(x => x != null).Select(Path.GetDirectoryName).Distinct().ToList();
        foreach (var location in locations)
        {
            var expectedPath = Path.Join(location, assemblyName + ".dll");
            if (!File.Exists(expectedPath)) continue;
            var assembly = Assembly.LoadFile(expectedPath);
            assemblies.Add(assembly);
            AddReferencedAssemblies(assembly, assemblies);
            return true;
        }

        return false;
    }

    private static void AddReferencedAssemblies(Assembly assembly, List<Assembly> assemblies)
    {
        var assemblyNames = assembly.GetReferencedAssemblies().ToList();
        var originalAssemblyFolder = Path.GetDirectoryName(assembly.Location);
        while (true)
        {
            var startingCount = assemblyNames.Count;
            for (int ii = 0; ii < assemblyNames.Count; ii++)
            {
                var referencedAssemblyName = assemblyNames[ii];
                Assembly referencedAssembly;
                try
                {
                    referencedAssembly = Assembly.Load(referencedAssemblyName);
                }
                catch
                {
                    var expectedPath = Path.Join(originalAssemblyFolder, referencedAssemblyName.Name + ".dll");
                    if (File.Exists(expectedPath)) referencedAssembly = Assembly.LoadFile(expectedPath);
                    else continue;
                }

                if (assemblies.Contains(referencedAssembly))
                {
                    // Possibly found alternative such as 2.1.0 for 2.0.0 .net standard   
                    continue;
                }
                assemblies.Add(referencedAssembly);
                var additionalAssemblies = referencedAssembly.GetReferencedAssemblies();
                foreach (var additionalAssembly in additionalAssemblies)
                {
                    if (assemblies.Any(x => x.FullName == additionalAssembly.FullName)) continue;
                    assemblyNames.Add(additionalAssembly);
                }
            }

            if (startingCount == assemblyNames.Count) break;
        }
    }
}
using System.Reflection;

namespace Quix.InteropGenerator.Writers.PythonWrapperWriter.Helpers;

public class AssemblyHelpers
{
    /// <summary>
    /// Retrieves the interop assembly name
    /// </summary>
    /// <param name="originalAssembly">The original assembly to convert</param>
    /// <returns>The interop assembly name</returns>
    public static string GetInteropAssemblyName(Assembly originalAssembly)
    {
        var assemblyName = CsharpInteropWriter.Helpers.AssemblyHelpers.GetAssemblyName(originalAssembly);
        if (string.IsNullOrWhiteSpace(assemblyName)) return "Interop"; // ????
        return assemblyName.Replace(".", "");
    }
}
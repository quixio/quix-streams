// YE BE WARNED, KRAKEN BE LIVIN' HERE
// TURN BACK WHILE YE CAN, 
// ⠀⠀⠀⠀⠀⠀⠀⠀⠀⣀⣤⣴⣶⣤⣄⠀⠀⠀⠀⠀⠀⠀⠀⠀
// ⠀⠀⠀⠀⣠⡤⣤⣄⣾⣿⣿⣿⣿⣿⣿⣷⣠⣀⣄⡀⠀⠀⠀⠀
// ⠀⠀⠀⠀⠙⠀⠈⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⡿⣬⡿⠀⠀⠀⠀
// ⠀⠀⠀⠀⠀⢀⣼⠟⢿⣿⣿⣿⣿⣿⣿⡿⠘⣷⣄⠀⠀⠀⠀⠀
// ⣰⠛⠛⣿⢠⣿⠋⠀⠀⢹⠻⣿⣿⡿⢻⠁⠀⠈⢿⣦⠀⠀⠀⠀
// ⢈⣵⡾⠋⣿⣯⠀⠀⢀⣼⣷⣿⣿⣶⣷⡀⠀⠀⢸⣿⣀⣀⠀⠀
// ⢾⣿⣀⠀⠘⠻⠿⢿⣿⣿⣿⣿⣿⣿⣿⣿⣷⣶⠿⣿⡁⠀⠀⠀
// ⠈⠙⠛⠿⠿⠿⢿⣿⡿⣿⣿⡿⢿⣿⣿⣿⣷⣄⠀⠘⢷⣆⠀⠀
// ⠀⠀⠀⠀⠀⢠⣿⠏⠀⣿⡏⠀⣼⣿⠛⢿⣿⣿⣆⠀⠀⣿⡇⡀
// ⠀⠀⠀⠀⢀⣾⡟⠀⠀⣿⣇⠀⢿⣿⡀⠈⣿⡌⠻⠷⠾⠿⣻⠁
// ⠀⠀⣠⣶⠟⠫⣤⠀⠀⢸⣿⠀⣸⣿⢇⡤⢼⣧⠀⠀⠀⢀⣿⠀
// ⠀⣾⡏⠀⡀⣠⡟⠀⠀⢀⣿⣾⠟⠁⣿⡄⠀⠻⣷⣤⣤⡾⠋⠀
// ⠀⠙⠷⠾⠁⠻⣧⣀⣤⣾⣿⠋⠀⠀⢸⣧⠀⠀⠀⠉⠁⠀⠀⠀
// ⠀⠀⠀⠀⠀⠀⠈⠉⠉⠹⣿⣄⠀⠀⣸⡿⠀⠀⠀⠀⠀⠀⠀⠀
//⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠙⠛⠿⠟⠛⠁⠀⠀⠀⠀⠀⠀⠀⠀
//
// But seriously, this code is made to work,
// not to be pretty

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using CommandLine;
using Quix.InteropGenerator.Writers.CsharpInteropWriter;
using Quix.InteropGenerator.Writers.CsharpInteropWriter.Helpers;
using Quix.InteropGenerator.Writers.Shared;


namespace Quix.InteropGenerator;

public class Program
{
    public class Options
    {
        [Option('a', "assembly", Required = true, HelpText = "Assembly path to reflect")]
        public string AssemblyPath { get; set; }
        
        [Option('o', "output", Required = true, HelpText = "Interop output path root")]
        public string OutputPathRoot { get; set; }
        
        [Option('c', "config", Required = true, HelpText = "Interop config path root")]
        public string ConfigPathRoot { get; set; }

        [Option('s', "singleassembly", Required = false, HelpText = "Whether to use a single C# assembly for saving output size")]
        public bool SingleAssembly { get; set; } = true;
    }
    
    public static async Task<int> Main(string[] args)
    {
        var result = Parser.Default.ParseArguments<Options>(args);
        if (result.Errors.Any())
        {
            return 1;
        }
        var options = result.Value;
        Console.WriteLine($"Relative assembly path: {options.AssemblyPath}");
        var assemblyPath = Path.GetFullPath(options.AssemblyPath);
        Console.WriteLine($"Absolute assembly path: {assemblyPath}");
        
        Console.WriteLine($"Relative output path: {options.OutputPathRoot}");
        if (Path.IsPathRooted(options.OutputPathRoot)) throw new Exception("Output path is not relative. In order to avoid catastrophic mistakes, such as deleting your entire mount, use relative.");
        var absoluteOutputPathRoot = Path.GetFullPath(options.OutputPathRoot);
        Console.WriteLine($"Absolute output path: {absoluteOutputPathRoot}");
        
        
        Console.WriteLine($"Relative config path: {options.ConfigPathRoot}");
        if (Path.IsPathRooted(options.ConfigPathRoot)) throw new Exception("Config path is not relative. In order to avoid catastrophic mistakes, such as deleting your entire mount, use relative.");
        var absoluteConfigPathRoot = Path.GetFullPath(options.OutputPathRoot);
        Console.WriteLine($"Absolute config path: {absoluteConfigPathRoot}");
        
        Console.WriteLine($"Single Assembly: {options.SingleAssembly}");
        
        var whitelist = new List<string>()
        {
            "^Quix\\.Sdk\\.*"
        }.Select(y => new Regex(y, RegexOptions.Compiled)).ToList();
        
        if (Directory.Exists(options.OutputPathRoot)) Directory.Delete(options.OutputPathRoot, true);
        Directory.CreateDirectory(options.OutputPathRoot);
        
        var assembly = System.Reflection.Assembly.LoadFile(assemblyPath);
        
        AppDomain.CurrentDomain.AssemblyResolve += (sender, eventArgs) =>
        {
            var containingFolder = Path.GetDirectoryName(eventArgs.RequestingAssembly.Location);
            var assemblyToLoadPath = Path.Combine(containingFolder, new AssemblyName(eventArgs.Name).Name + ".dll");
            if (!File.Exists(assemblyToLoadPath)) return null;
            return Assembly.LoadFrom(assemblyToLoadPath);
        };

        // C#
        var writtenDetails = await WriteCsharp(options, assembly, whitelist);
        
        // Python
        await WritePython(options, writtenDetails);
        return 0;
    }

    private static async Task<CsharpWrittenDetails> WriteCsharp(Options options, Assembly assembly, List<Regex> whitelist)
    {
        var csharpPath = Path.Combine(options.OutputPathRoot, "Csharp");

        var utilPath = options.SingleAssembly
            ? AssemblyHelpers.GetSingleAssemblyBasePath(csharpPath, assembly)
            : csharpPath;

        // utils
        var utilsWriter = new UtilsAssemblyWriter(utilPath, options.SingleAssembly);
        await utilsWriter.WriteContent();
        
        // Assembly and dependencies
        var writtenDetails = new CsharpWrittenDetails();
        var writer = new AssemblyWriter(assembly, writtenDetails, options.SingleAssembly, csharpPath, options.ConfigPathRoot, utilsWriter.ProjectPath, 2, whitelist, 1);
        await writer.WriteContent();

        return writtenDetails;
    }
    
    private static async Task WritePython(Options options, CsharpWrittenDetails writtenDetails)
    {
        var pythonPath = Path.Combine(options.OutputPathRoot, "Python");
        
        // utils
        var utilsWriter = new Writers.PythonWrapperWriter.UtilsAssemblyWriter(pythonPath);
        var typeLookups = await utilsWriter.WriteContent();

        // assembly and dependencies
        var PythonWrapperWriter = new Writers.PythonWrapperWriter.AssemblyWriter(writtenDetails, pythonPath);
        await PythonWrapperWriter.WriteContent(typeLookups);
    }
}
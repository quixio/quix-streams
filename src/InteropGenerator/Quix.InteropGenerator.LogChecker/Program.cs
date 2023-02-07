// See https://aka.ms/new-console-template for more information

using CommandLine;

namespace Quix.InteropGenerator;

public class Program
{

    public class Options
    {
        [Option('f', "file", Required = true, HelpText = "Path to the file to analyze")]
        public string FilePath { get; set; }
    }


    public static async Task<int> Main(string[] args)
    {

        var result = Parser.Default.ParseArguments<Options>(args);
        if (result.Errors.Any())
        {
            return 1;
        }

        var options = result.Value;
        var path = options.FilePath;
        
        Console.WriteLine($"File path: {path}");

        var file = Path.GetFileName(path);
        if (file.Contains("*latest*"))
        {
            var directory = Path.GetDirectoryName(path);
            var start = file.Substring(0, file.IndexOf("*latest*"));
            var end = file.Substring(file.IndexOf("*latest*") + "*latest*".Length);
            var fileToUse = Directory.GetFiles(directory)
                .Where(y => Path.GetFileName(y).StartsWith(start) && Path.GetFileName(y).EndsWith(end))
                .OrderByDescending(y => File.GetCreationTime(y)).FirstOrDefault();
            if (string.IsNullOrEmpty(fileToUse))
            {
                Console.WriteLine($"No File matching filter exist.");
                return 3;
            }
            Console.WriteLine($"Latest file: {fileToUse}");

            path = fileToUse;
        }

        if (!File.Exists(path))
        {
            Console.WriteLine($"File does not exist.");
            return 2;
        }

        var evaluator = new Evaluator();
        await foreach (var parsed in new LogParser().ParseFile(path))
        {
            evaluator.Add(parsed);
        }

        evaluator.Evaluate();

        return 0;
    }


}
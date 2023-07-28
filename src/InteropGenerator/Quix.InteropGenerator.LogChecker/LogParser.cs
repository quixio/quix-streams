using System.Reflection;
using System.Text.RegularExpressions;

namespace Quix.InteropGenerator;

public class LogParser
{
    private Dictionary<Type, Func<string, bool>> parsers = AppDomain.CurrentDomain.GetAssemblies()
        .SelectMany(s => s.GetTypes()).Where(y => !y.IsInterface && typeof(ILineParser).IsAssignableFrom(y)).ToDictionary(y => y, y =>
        {
            var method = y.GetMethod(nameof(ILineParser.CanParse));
            if (method == null) throw new Exception("Not implementing CanParse of ILineParser in " + y.FullName);
            return new Func<string, bool>(s => (bool)method.Invoke(null, new[] { s }));
        });

    public async IAsyncEnumerable<ILineParser> ParseFile(string filePath)
    {
        using var fs = File.OpenRead(filePath);
        using var sr = new StreamReader(fs);
        int lineNumber = 1;
        while (!sr.EndOfStream)
        {
            var line = await sr.ReadLineAsync();
            if (line!.Contains('\0'))
            {
                line = line.Replace("\0", "");
                Console.WriteLine($"Line {lineNumber} has '\\0' char in it, expect problems");
            }
            
            const string timeSplitText = "]  ";
            var splitIndex = line.IndexOf(timeSplitText);
            if (splitIndex > -1)
            {
                var timePart = line.Substring(0, splitIndex + 1);
                var linePart = line.Substring(splitIndex + timeSplitText.Length).TrimStart();
                line = linePart;
            }
            foreach (var parserPair in parsers)
            {
                if (parserPair.Value(line))
                {
                    var instance = (ILineParser)Activator.CreateInstance(parserPair.Key, lineNumber, line);
                    yield return instance;
                    break;
                }
            }

            lineNumber++;
        }
    }

    public interface ILineParser
    {
        static bool CanParse(string line) => throw new NotImplementedException();
        
        int LineNumber { get; }
    }

    public class PtrAllocationLineParser : ILineParser
    {
        private static Regex LineRegex = new Regex("^Allocated (Pinned )?U?Ptr: (\\d+), type: (.+), is", RegexOptions.Compiled);
        
        
        public PtrAllocationLineParser(int lineNumber, string line)
        {
            this.LineNumber = lineNumber;
            var match = LineRegex.Match(line);

            this.Ptr = match.Groups[2].Value;
            this.Type = match.Groups[3].Value;
        }
        
        public string Ptr { get; } // technically int, but not necessary for us, only possible perf impr
        
        public string Type { get; }
        
        public int LineNumber { get; }
        
        public static bool CanParse(string line)
        {
            return LineRegex.IsMatch(line);
        }
    } 
    
    public class PtrFreedLineParser : ILineParser
    {
        private static Regex LineRegex = new Regex("^Freed U?Ptr: (\\d+)", RegexOptions.Compiled);
        
        
        public PtrFreedLineParser(int lineNumber, string line)
        {
            this.LineNumber = lineNumber;
            var match = LineRegex.Match(line);

            this.Ptr = match.Groups[1].Value;
        }
        
        public string Ptr { get; } // technically int, but not necessary for us, only possible perf impr
        
        public int LineNumber { get; }
        
        public static bool CanParse(string line)
        {
            return LineRegex.IsMatch(line);
        }
    } 
}
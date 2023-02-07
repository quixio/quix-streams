using System;
using System.Text;
using System.Threading.Tasks;

namespace Quix.InteropGenerator.Writers.Shared;

public abstract class BaseWriter
{
    public abstract Task WriteContent(Func<string, Task> writeLineAction);
    
    public override string ToString()
    {
        var sb = new StringBuilder();
        WriteContent((line) =>
        {
            sb.AppendLine(line);
            return Task.CompletedTask;
        }).GetAwaiter().GetResult();
        return sb.ToString();
    }
}
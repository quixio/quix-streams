using System;
using System.Threading.Tasks;

namespace Quix.InteropGenerator.Writers.Shared;

public interface IContentWriter
{
    void Write(string line);
    void WriteEmptyLine(bool force = false);
}

public interface IIndentWriter
{
    void IncrementIndent();
    void DecrementIndent();
}

public class IndentContentWriter : IIndentWriter, IContentWriter
{
    private readonly Func<string, Task> writeFunc;
    private int indent;
    private bool isLastEmpty;
    private const int IndentSize = 4;

    public IndentContentWriter(Func<string, Task> writeFunc, int startingIndent = 0)
    {
        this.writeFunc = writeFunc;
        this.indent = startingIndent;
    }
    
    void IContentWriter.Write(string line)
    {
        this.Write(line).RunSynchronously();
    }

    void IContentWriter.WriteEmptyLine(bool force)
    {
        this.WriteEmptyLine(false).RunSynchronously();
    }
    
    public Task Write(string line)
    {
        isLastEmpty = false;
        if (indent <= 0) return writeFunc(line);
        return writeFunc(new string(' ', indent) + line);
    }

    public Task WriteEmptyLine(bool force = false)
    {
        if (isLastEmpty && !force) return Task.CompletedTask;
        isLastEmpty = true;
        if (indent <= 0) return writeFunc("");
        return writeFunc(new string(' ', indent));
    }

    public void IncrementIndent()
    {
        indent += IndentSize;
    }

    public void DecrementIndent()
    {
        if (indent > IndentSize) indent -= IndentSize;
        else indent = 0;
    }
}
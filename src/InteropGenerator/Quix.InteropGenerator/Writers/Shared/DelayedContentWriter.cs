using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Quix.InteropGenerator.Writers.Shared;

public class DelayedWriter : IIndentWriter, IContentWriter
{
    private Func<string, Task> writeFunc;
    private int indent = 0;
    private bool isLastEmpty = false;
    private const int indentSize = 4;
    private List<Func<Task>> actions = new List<Func<Task>>();

    public bool IsEmpty { get; private set; } = true;

    public DelayedWriter( int startingIndent = 0)
    {
        this.indent = startingIndent;
    }
    
    public void Write(string line)
    {
        IsEmpty = false;
        actions.Add(() =>
            {
                isLastEmpty = false;
                if (indent <= 0) return writeFunc(line);
                return writeFunc(new string(' ', indent) + line);
            }
        );
    }
    
    public Task WriteAsync(string line)
    {
        Write(line);
        return Task.CompletedTask;
    }

    public void WriteEmptyLine(bool force = false)
    {
        actions.Add(() =>
        {
            if (isLastEmpty && !force) return Task.CompletedTask;
            isLastEmpty = true;
            if (indent <= 0) return writeFunc("");
            return writeFunc(new string(' ', indent));
        });
    }

    public void IncrementIndent()
    {
        actions.Add(() =>
        {
            indent += indentSize;
            return Task.CompletedTask;
        });
    }

    public void DecrementIndent()
    {
        actions.Add(() =>
        {
            if (indent > indentSize) indent -= indentSize;
            else indent = 0;
            return Task.CompletedTask;
        });
    }

    public async Task Flush(Func<string, Task> writeFunc)
    {
        this.writeFunc = writeFunc;
        foreach (var action in actions)
        {
            await action();
        }
    } 
    
    public Task Flush(Action<string> writeFunc)
    {
        return this.Flush((input) =>
        {
            writeFunc(input);
            return Task.CompletedTask;
        });
    } 
}
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Quix.InteropGenerator.Writers.PythonWrapperWriter.Helpers;
using Quix.InteropGenerator.Writers.Shared;

namespace Quix.InteropGenerator.Writers.PythonWrapperWriter;

public class EnumWriter : BaseWriter
{
    private readonly Type type;

    public EnumWriter(Type type)
    {
        this.type = type;
    }

    public override async Task WriteContent(Func<string, Task> writeLineAction)
    {
        var backingType = Enum.GetUnderlyingType(type);
        var indentedWriter = new IndentContentWriter(writeLineAction, 0);
        Dictionary<Type, Func<Task>> backingTypeHandlers = new Dictionary<Type, Func<Task>>()
        {
            {typeof(int), async () =>
            {
                var names = Enum.GetNames(this.type);
                for (var index = 0; index < names.Length; index++)
                {
                    var name = names[index];
                    var value = (int)Enum.Parse(type, name);
                    if (PythonUtils.IsReservedWord(name)) name += "_";
                    await indentedWriter.Write(name + " = " + value);
                }
            }}
        };
        if (!backingTypeHandlers.TryGetValue(backingType, out var handler)) throw new NotImplementedException($"Enum with backing type of {type.FullName} is not supported");
        await indentedWriter.Write("from enum import Enum");
        await indentedWriter.WriteEmptyLine();
        await indentedWriter.Write("class " + type.Name + "(Enum):");
        {
            indentedWriter.IncrementIndent();
            await handler();
            indentedWriter.DecrementIndent();
        }
    }
}
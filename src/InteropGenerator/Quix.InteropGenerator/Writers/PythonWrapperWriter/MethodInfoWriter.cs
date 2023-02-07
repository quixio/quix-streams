using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using InteropHelpers.Interop;
using Quix.InteropGenerator.Writers.Shared;

namespace Quix.InteropGenerator.Writers.PythonWrapperWriter;

public class MethodInfoWriter : BaseWriter
{
    private readonly TypeWriter typeWriter;
    private readonly MethodWrittenDetails methodWrittenDetails;
    private readonly HashSet<Type> usings;
    private readonly string declaringTypePythonName;

    public MethodInfoWriter(TypeWriter typeWriter, MethodWrittenDetails methodWrittenDetails, HashSet<Type> usings)
    {
        this.typeWriter = typeWriter;
        this.methodWrittenDetails = methodWrittenDetails;
        this.declaringTypePythonName = typeWriter.TypePythonName;
        this.usings = usings;
    }

    public override async Task WriteContent(Func<string, Task> writeLineAction)
    {
        var indentedWriter = new IndentContentWriter(writeLineAction, 0);
        
        await indentedWriter.WriteEmptyLine();
        await WriteFunctionHeader(indentedWriter);
        await WriteFunctionBody(indentedWriter);
    }

    private async Task WriteFunctionBody(IndentContentWriter contentWriter)
    {
        contentWriter.IncrementIndent();
        var paramNames = new Dictionary<string, string>();
        var anyRecast = false;

        string instanceArgName = null;
        if (!this.methodWrittenDetails.MethodBase.IsStatic && !this.methodWrittenDetails.MethodBase.IsConstructor)
        {
            instanceArgName = "self._pointer";
            paramNames[instanceArgName] = instanceArgName;
        }
        
        foreach (var parameterInfo in this.methodWrittenDetails.MethodParameterOrder)
        {
            if (parameterInfo.IsOut)
            {
                paramNames[parameterInfo.Name] = parameterInfo.Name + "Out";
                continue;
            }
            var result = await TryWriteTypeConversion(contentWriter, parameterInfo.ParameterType, parameterInfo.Name, null, true);
            if (result.Item1)
            {
                paramNames[parameterInfo.Name] = result.Item2;
                anyRecast = true;
            }
            else paramNames[parameterInfo.Name] = parameterInfo.Name;
        }

        if (anyRecast) await contentWriter.WriteEmptyLine();
        
        
        var sb = new StringBuilder();
        var resultName = string.Empty;
        if (this.methodWrittenDetails.ReturnType != typeof(void))
        {
            resultName = "result";
            var counter = 1;
            while(paramNames.Any(y => y.Key == resultName || y.Value == resultName))
            {
                counter++;
                resultName = "result" + counter;
            }

            sb.Append(resultName);
            sb.Append(" = ");
        }
        
        sb.Append("InteropUtils.invoke(\"");
        sb.Append(this.methodWrittenDetails.EntryPoint);
        sb.Append("\"");
        
        if (instanceArgName != null)
        {
            sb.Append(", ");
            sb.Append(paramNames[instanceArgName]);
        }

        // If there are additional things to return to avoid getting them GC'd
        var returnValues = new List<string>();
        
        foreach (var parameterInfo in this.methodWrittenDetails.MethodParameterOrder)
        {
            var pythonType = Utils.GetPythonMappableType(parameterInfo.ParameterType);
            var isEnum = Utils.IsEnum(pythonType);
            var isDelegate = !isEnum && Utils.IsDelegate(parameterInfo.ParameterType);
            var name = paramNames[parameterInfo.Name];
            
            sb.Append(", ");
            
            if (isEnum && Nullable.GetUnderlyingType(parameterInfo.ParameterType) != null)
            {
                sb.Append($"None if {name} is None else ");
            }
            
            sb.Append(name);
            

            if (isEnum)
            {
                sb.Append(".value");
            }
            else if (isDelegate)
            {
                returnValues.Add(name);
            }
        }

        sb.Append(")");

        if (this.declaringTypePythonName == "IStreamReader")
        {
            
        }

        await contentWriter.Write(sb.ToString());
        if (!string.IsNullOrWhiteSpace(resultName))
        {
            var result = await TryWriteTypeConversion(contentWriter, this.methodWrittenDetails.ReturnType, resultName, null, false);
            if (result.Item1) resultName = result.Item2;
            returnValues.Insert(0, resultName);
        }

        if (returnValues.Count > 0)
        {
            await contentWriter.Write($"return {string.Join(", ", returnValues)}");
        }
        
        contentWriter.DecrementIndent();
    }

    private async Task<Tuple<bool, string>> TryWriteTypeConversion(IndentContentWriter contentWriter, Type dotnetType, string sourceName, string targetName = null, bool convertingToDotnet = true)
    {

        Type target, source;
        if (convertingToDotnet)
        {
            target = Utils.GetUnmanagedType(dotnetType);
            source = Utils.GetPythonMappableType(dotnetType);
        }
        else
        {
            source = Utils.GetUnmanagedType(dotnetType);
            target = Utils.GetPythonMappableType(dotnetType);
        }
         
        if (Utils.IsDelegate(dotnetType))
        {
            if (convertingToDotnet) target = dotnetType;
            else source = dotnetType;
        }

        var nullableTargetType = Nullable.GetUnderlyingType(target);
        if (nullableTargetType == null && target == source)
        {
            if (convertingToDotnet && target == typeof(long))
            {
                targetName ??= $"{sourceName}_long";
                await contentWriter.Write($"{targetName} = ctypes.c_longlong({sourceName})");
                return new Tuple<bool, string>(true, targetName);
            }

            // Even with the return type set, without explicitly converting to c_void_p, issues rose when passed back to .net as reference such as int too long
            if (!convertingToDotnet && source == typeof(IntPtr))
            {
                targetName ??= $"{sourceName}_ptr";
                await contentWriter.Write($"{targetName} = ctypes.c_void_p({sourceName}) if {sourceName} is not None else None");
                return new Tuple<bool, string>(true, targetName);
            }
            return new Tuple<bool, string>(false, null);
        }
        
        var nullableSourceType = Nullable.GetUnderlyingType(target);
        
        if (nullableTargetType != null)
        {
            if (!convertingToDotnet)
            {
                targetName ??= $"{sourceName}_optional";
                await contentWriter.Write($"{targetName} = None if not {sourceName}.HasValue else {sourceName}.Value");
                return new Tuple<bool, string>(true, targetName);
            }
            
            var convertedVarName = sourceName; // in case conversion is necessary, we'll use the updated name
            if (nullableSourceType != null && !InteropUtils.IsBlittableType(nullableSourceType))
            {
                if (nullableSourceType.IsEnum)
                {
                    convertedVarName = $"{convertedVarName}_conv";
                    await contentWriter.Write($"{convertedVarName} = {sourceName}.value");   
                }
                else
                {
                    throw new NotImplementedException("Currently only able to deal with enum when converting from nullable/optional type to dotnet and type is not blittable.");
                }
            }
            targetName ??= $"{sourceName}_nullable";
            await contentWriter.Write($"{targetName} = {GetCTypeString(target)}({convertedVarName})");
            return new Tuple<bool, string>(true, targetName);
        }

        if (source == typeof(bool))
        {
            targetName ??= $"{sourceName}_bool";
            await contentWriter.Write($"{targetName} = 1 if {sourceName} else 0");
            return new Tuple<bool, string>(true, targetName);
        }
        
        
        if (source == typeof(IntPtr))
        {
            if (target == typeof(string))
            {
                await contentWriter.Write($"{sourceName} = c_void_p({sourceName}) if {sourceName} is not None else None");
                targetName ??= $"{sourceName}_str";
                await contentWriter.Write($"{targetName} = InteropUtils.uptr_to_utf8({sourceName})");
                return new Tuple<bool, string>(true, targetName);
            }

            targetName ??= $"{sourceName}_inst";
            var typeText = typeWriter.LookupTypeAsText(target);
            await contentWriter.Write($"{targetName} = TODO<{typeText}>({sourceName})");
            return new Tuple<bool, string>(true, targetName);
        }

        if (target == typeof(IntPtr))
        {
            if (source == typeof(string))
            {
                targetName ??= $"{sourceName}_ptr";
                await contentWriter.Write($"{targetName} = InteropUtils.utf8_to_ptr({sourceName})"); // using normal GC-able ptr here, as lifetime is the function context only
                return new Tuple<bool, string>(true, targetName);
            }

            targetName ??= $"{sourceName}_ptr";
            var typeText = typeWriter.LookupTypeAsText(source);
            await contentWriter.Write($"{targetName} = TODO<{typeText}>({sourceName})");
            return new Tuple<bool, string>(true, targetName);
        }

        // When the function is coming from dotnet and needs to be turned into a function in python
        if (typeof(Delegate).IsAssignableFrom(target) && source.BaseType == typeof(MulticastDelegate))
        {
            return new Tuple<bool, string>(true, "Exception(\"NOT IMPLEMENTED YET\")"); // TODO
        }
        
        // When the function is coming from python and needs to be turned into a function in dotnet
        if (typeof(Delegate).IsAssignableFrom(source) &&  target.BaseType == typeof(MulticastDelegate))
        {
            var delegateMethodInfo = Utils.GetMulticastDelegateMethodInfo(target);
            var wrapperSb = new StringBuilder();
            var addressName = $"{sourceName}_func_wrapper_addr";
            wrapperSb.Append(addressName);
            wrapperSb.Append(" = ");
            wrapperSb.Append("None");
            await contentWriter.Write(wrapperSb.ToString());
            wrapperSb.Clear();
            await contentWriter.Write($"if {sourceName} is not None:");
            contentWriter.IncrementIndent();

            var delegateParameters = delegateMethodInfo.GetParameters();
            
            // conversion wrapper which converts to the expected type, as by default the type isn't right, such as getting int instead of c_void_p
            targetName = $"{sourceName}_converter";
            wrapperSb.Append(targetName);
            wrapperSb.Append(" = ");
            wrapperSb.Append($"lambda ");
            for (var index = 0; index < delegateParameters.Length; index++)
            {
                if (index > 0) wrapperSb.Append($", ");
                wrapperSb.Append($"p{index}");
            }

            wrapperSb.Append($": {sourceName}(");
            for (var index = 0; index < delegateParameters.Length; index++)
            {
                var parameterInfo = delegateParameters[index];
                var typeString = this.typeWriter.GetPythonTypeString(parameterInfo.ParameterType);
                if (index > 0) wrapperSb.Append(", ");
                wrapperSb.Append($"{typeString}(p{index})");
            }

            wrapperSb.AppendLine(")");
            await contentWriter.Write(wrapperSb.ToString());
            wrapperSb.Clear();
            sourceName = targetName;
            
            // wrapper func
            targetName = $"{sourceName}_func_wrapper";
            wrapperSb.Append(targetName);
            wrapperSb.Append(" = ");
            wrapperSb.Append($"ctypes.CFUNCTYPE(");
            wrapperSb.Append(GetCTypeString(delegateMethodInfo.ReturnType));
            
            for (var index = 0; index < delegateParameters.Length; index++)
            {
                var parameterInfo = delegateParameters[index];
                wrapperSb.Append(", ");
                wrapperSb.Append(GetCTypeString(parameterInfo.ParameterType));
            }
            wrapperSb.Append(")(");
            wrapperSb.Append(sourceName);
            wrapperSb.Append(")");

            await contentWriter.Write(wrapperSb.ToString());
            wrapperSb.Clear();
            wrapperSb.Append(addressName);
            wrapperSb.Append(" = ");
            wrapperSb.Append($"ctypes.cast({targetName}, c_void_p)");
            await contentWriter.Write(wrapperSb.ToString());

            await contentWriter.Write("if InteropUtils.DebugEnabled:");
            contentWriter.IncrementIndent();
            await contentWriter.Write($"print(\"Registering {targetName} in {this.methodWrittenDetails.EntryPoint}, addr {{}}\".format({addressName}))");
            await contentWriter.Write($"{addressName}_val = {addressName}.value");
            await contentWriter.Write($"# TODO {this.declaringTypePythonName}._weakrefs.append(weakref.ref({targetName}, lambda x: print(\"De-referenced {targetName} in {this.methodWrittenDetails.EntryPoint}, addr {{}}\".format({addressName}_val))))");
            contentWriter.DecrementIndent();
            contentWriter.DecrementIndent();
            return new Tuple<bool, string>(true, addressName);
        }
        
        return new Tuple<bool, string>(false, null);
    }

    private async Task WriteFunctionHeader(IndentContentWriter contentWriter)
    {
        var headerWriter = new DelayedWriter(0);
        var delayedWriter = new DelayedWriter(0);
        var sb = new StringBuilder();
        sb.Append("def ");
        sb.Append(this.methodWrittenDetails.UniqueMethodName);
        sb.Append("(");
        
        bool requiresParamRedeclaration = false;
        var ctypesParams = new List<Type>(); 


        var first = true;
        if (!this.methodWrittenDetails.MethodBase.IsStatic && !this.methodWrittenDetails.MethodBase.IsConstructor)
        {
            first = false;
            sb.Append("self");
            ctypesParams.Add(typeof(IntPtr));
            requiresParamRedeclaration = true;
        }

            
        foreach (var parameterInfo in this.methodWrittenDetails.MethodParameterOrder)
        {
            if (!first) sb.Append(", ");
            first = false;
            sb.Append(parameterInfo.Name);
            sb.Append(": ");
            var pyType = typeWriter.GetPythonTypeString(parameterInfo.ParameterType);
            sb.Append(pyType);
            if (parameterInfo.IsOptional)
            {
                sb.Append(" = ");
                if (parameterInfo.HasDefaultValue)
                {
                    if (parameterInfo.DefaultValue != null && parameterInfo.DefaultValue.GetType() == typeof(bool)) sb.Append(((bool)parameterInfo.DefaultValue) ? "True" : "False");
                    else if (parameterInfo.DefaultValue != null && parameterInfo.DefaultValue is Enum) sb.Append(parameterInfo.DefaultValue.GetType().Name + "." + parameterInfo.DefaultValue);
                    else sb.Append("None");
                }
                else sb.Append("None");
            }
            AddTypeUsing(parameterInfo.ParameterType, false); // TODO possibly could use the python variant if we have the class rather than a pointer if ref type
            delayedWriter.WriteEmptyLine();            
            delayedWriter.WriteEmptyLine();
            delayedWriter.Write($"{parameterInfo.Name}: {pyType}");
            delayedWriter.IncrementIndent();
            var prefix = string.Empty;
            if (parameterInfo.IsOptional) prefix = "(Optional) ";
            var suffix = string.Empty;
            if (parameterInfo.HasDefaultValue)
            {
                suffix = ". Defaults to " + parameterInfo.DefaultValue;
                if (parameterInfo.DefaultValue == null) suffix += "None";
            }
            if (pyType == "c_void_p")
            {
                requiresParamRedeclaration = true;
                delayedWriter.Write($"{prefix}GC Handle Pointer to .Net type {this.methodWrittenDetails.GetDotNetTypeAsText(parameterInfo.ParameterType, false)}{suffix}");
            }
            else
            {
                if (pyType == "float") requiresParamRedeclaration = true; // apparently it has to be specified
                delayedWriter.Write($"{prefix}Underlying .Net type is {this.methodWrittenDetails.GetDotNetTypeAsText(parameterInfo.ParameterType, false)}{suffix}");
            }
            
            delayedWriter.DecrementIndent();
            ctypesParams.Add(parameterInfo.ParameterType);            
        }

        
        // TODO Add multiple return type hint somehow
        sb.Append(") -> ");
        var pyReturnType = typeWriter.GetPythonTypeString(this.methodWrittenDetails.ReturnType);
        sb.Append(pyReturnType);
        AddTypeUsing(this.methodWrittenDetails.ReturnType, true);
        sb.Append(":");
        if (this.methodWrittenDetails.MethodBase.IsStatic || this.methodWrittenDetails.MethodBase.IsConstructor)
        {
            headerWriter.Write("@staticmethod");
        }

        headerWriter.Write(sb.ToString());
        headerWriter.IncrementIndent();
        headerWriter.Write("\"\"\"");
        headerWriter.Write("Parameters");
        headerWriter.Write("----------");
        await delayedWriter.Flush(headerWriter.Write);
        headerWriter.WriteEmptyLine();
        headerWriter.Write("Returns");
        headerWriter.Write("-------");

        if (this.methodWrittenDetails.ReturnType == typeof(void))
        {
            headerWriter.Write("None:");
            headerWriter.IncrementIndent();            
            headerWriter.Write("Underlying .Net type is void");
            headerWriter.DecrementIndent();
        }
        else
        {
            headerWriter.WriteEmptyLine();
            headerWriter.Write($"{pyReturnType}:");
            headerWriter.IncrementIndent();
            if (pyReturnType == "c_void_p")
            {
                headerWriter.Write($"GC Handle Pointer to .Net type {this.methodWrittenDetails.GetDotNetTypeAsText(this.methodWrittenDetails.ReturnType, false)}");
            }
            else headerWriter.Write($"Underlying .Net type is {this.methodWrittenDetails.GetDotNetTypeAsText(this.methodWrittenDetails.ReturnType, false)}");
            headerWriter.DecrementIndent();
        }
        headerWriter.Write("\"\"\"");
        headerWriter.DecrementIndent();

        // We might need to update the underlying function to use the proper return/param types, as some types are not automatically picked up correctly by ctypes
        var delayedPreHeaderWriter = new DelayedWriter(0);


        var unmanagedReturnType = Utils.GetUnmanagedType(this.methodWrittenDetails.ReturnType);
        delayedPreHeaderWriter.Write("_interop_func.restype = " + this.GetCTypeString(unmanagedReturnType));

        if (requiresParamRedeclaration)
        {
            delayedPreHeaderWriter.Write("_interop_func.argtypes = [" + string.Join(", ", ctypesParams.Select(Utils.GetUnmanagedType).Select(GetCTypeString)) + "]");
        }

        if (!delayedPreHeaderWriter.IsEmpty)
        {
            await contentWriter.Write("# ctypes function return type//parameter fix");
            await contentWriter.Write($"_interop_func = InteropUtils.get_function(\"{this.methodWrittenDetails.EntryPoint}\")");
            await delayedPreHeaderWriter.Flush(contentWriter.Write);
        }


        await headerWriter.Flush(contentWriter.Write);
    }

    private void AddTypeUsing(Type type, bool isPythonType)
    {
        if (!isPythonType)
        {
            foreach (var usedType in Utils.GetUsedTypes(type).Select(Utils.GetUnmanagedType))
            {
                this.usings.Add(usedType);
            }

            return;
        }
        foreach (var usedType in Utils.GetUsedTypes(type))
        {
            this.usings.Add(usedType);
        }
        
    }

    private string GetCTypeString(Type type)
    {
        if (Nullable.GetUnderlyingType(type) != null)
        {
            return $"InteropUtils.create_nullable({GetCTypeString(Nullable.GetUnderlyingType(type))})";
        }
        var pythonType = Utils.GetPythonMappableType(type);
        if (Utils.IsEnum(pythonType))
        {
            type = Enum.GetUnderlyingType(pythonType);
        }
        var pythonString = typeWriter.GetPythonTypeString(type);
        switch (pythonString)
        {
            case "int":
                if (type == typeof(long))
                    return "ctypes.c_longlong";
                else if (type == typeof(ulong))
                    return "ctypes.c_ulonglong";
                else if (type == typeof(byte))
                    return "ctypes.c_ubyte"; // probably?
                else if (type == typeof(uint))
                    return "ctypes.c_uint";
                return "ctypes.c_int";
            case "str":
                return "ctypes.c_char_p";
            case "bool":
                return "ctypes.c_bool";
            case "float":
                if (type == typeof(double)) return "ctypes.c_double";
                return "ctypes.c_float";
            case "None":
            case "c_void_p":
                return pythonString;
        }

        if (pythonString.StartsWith("Callable")) return "c_void_p"; // TODO?
        return pythonString;
    }
}
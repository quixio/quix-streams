using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using InteropHelpers.Interop;
using InteropHelpers.Interop.ExternalTypes.System;
using Quix.InteropGenerator.Writers.CsharpInteropWriter.Helpers;
using Quix.InteropGenerator.Writers.Shared;

namespace Quix.InteropGenerator.Writers.CsharpInteropWriter;

/// <summary>
/// Method (not MethodInfo) writer, as it can use more than just MethodInfo
/// </summary>
public class MethodWriter : BaseWriter
{
    private readonly TypeWriter typeWriter;
    private readonly MethodBase methodBase;
    private readonly ParameterInfo[] parameterInfos;
    private readonly Type returnType;
    private readonly string nameSuffix;
    private readonly MethodWrittenDetails methodWrittenDetails;
    private readonly FieldInfo fieldInfo;
    private readonly ParameterInfo[] methodParameterOrder;

    /// <summary>
    /// 
    /// </summary>
    /// <param name="typeWriter"></param>
    /// <param name="methodBase"></param>
    /// <param name="nameSuffix"></param>
    /// <param name="methodWrittenDetails"></param>
    /// <param name="fieldInfo">Optional field info where the method is actually a field</param>
    public MethodWriter(TypeWriter typeWriter, MethodBase methodBase, string nameSuffix, MethodWrittenDetails methodWrittenDetails, FieldInfo fieldInfo = null)
    {
        this.typeWriter = typeWriter;
        this.methodBase = methodBase;
        this.parameterInfos = methodBase.GetParameters();
        this.methodParameterOrder = this.parameterInfos.OrderBy(y=> y.IsOptional).ToArray(); // always have non-optional first;
        this.returnType = methodBase is ConstructorInfo ci 
            ? ci.DeclaringType
            : methodBase is MethodInfo mi 
                ? mi.ReturnType
                : typeof(void);
        this.nameSuffix = nameSuffix;
        this.methodWrittenDetails = methodWrittenDetails;
        this.fieldInfo = fieldInfo;
        this.methodWrittenDetails.DeclaringType = this.typeWriter.Type;
        this.methodWrittenDetails.UniqueMethodName = this.GetMethodName();
        this.methodWrittenDetails.EntryPoint = GetEntryPointName();        
        this.methodWrittenDetails.MethodBase = this.methodBase;
        this.methodWrittenDetails.ReturnType = this.returnType;
        this.methodWrittenDetails.ParameterInfos = this.parameterInfos;
        this.methodWrittenDetails.MethodParameterOrder = this.methodParameterOrder;
    }

    public override async Task WriteContent(Func<string, Task> writeLineAction)
    {
        var preFunctionWriter = new DelayedWriter(0);
        var writer = new DelayedWriter(0);
        
        writer.WriteEmptyLine();
        var args = WriteFunctionHeader(writer);
        WriteFunctionBody(writer, args, preFunctionWriter);

        if (!preFunctionWriter.IsEmpty)
        {
            await writeLineAction(""); // empty line
            await preFunctionWriter.Flush(writeLineAction);
        }
        await writer.Flush(writeLineAction);
    }

    private void WriteFunctionBody(DelayedWriter writer, Dictionary<string, string> argNames, DelayedWriter preFunctionWriter)
    {
        using var iw = new IndentWriter<DelayedWriter>(writer);
        writer.Write($"InteropUtils.LogDebug($\"Invoking entrypoint {this.methodWrittenDetails.EntryPoint}\");");
        using var ehw = new ExceptionHandlerWriter<DelayedWriter>(writer, this.methodWrittenDetails.ReturnType != typeof(void), delayedWriter =>
        {
            delayedWriter.Write($"InteropUtils.LogDebug(\"Exception in {this.methodWrittenDetails.EntryPoint}\");");
            foreach (var argName in argNames)
            {
                if (argName.Value.Contains("delegate*"))
                {
                    delayedWriter.Write($"InteropUtils.LogDebug($\"Arg {argName.Key} ({argName.Value}) has value {{(IntPtr){argName.Key}}}\");");
                }
                else delayedWriter.Write($"InteropUtils.LogDebug($\"Arg {argName.Key} ({argName.Value}) has value: {{{argName.Key}}}\");");   
            }
        });

        var paramNames = new Dictionary<string, string>();
        var anyRecast = false;

        var instanceArgName = Utils.GetInstanceParameterName(this.methodWrittenDetails.DeclaringType, this.methodBase);
        if (instanceArgName != null)
        {
            var result = WriteTypeConversion(writer, preFunctionWriter, this.methodBase.DeclaringType, false, instanceArgName);
            paramNames[instanceArgName] = result;
            anyRecast |= instanceArgName != result;
        }
        
        foreach (var parameterInfo in this.parameterInfos)
        {
            if (parameterInfo.IsOut)
            {
                paramNames[parameterInfo.Name] = parameterInfo.Name + "Out";
                continue;
            }
            var result = WriteTypeConversion(writer, preFunctionWriter, parameterInfo.ParameterType, false, parameterInfo.Name);
            paramNames[parameterInfo.Name] = result;
            anyRecast |= instanceArgName != result;

            paramNames[parameterInfo.Name] = WriteDefaultValue(writer, parameterInfo, paramNames[parameterInfo.Name]);
        }

        if (anyRecast) writer.WriteEmptyLine();

        var sb = new StringBuilder();
        var resultName = string.Empty;
        if (this.returnType != typeof(void))
        {
            resultName = "result";
            var counter = 1;
            while(paramNames.Any(y => y.Key == resultName || y.Value == resultName))
            {
                counter++;
                resultName = "result" + counter;
            }

            sb.Append("var ");
            sb.Append(resultName);
            sb.Append(" = ");
        }

        var lineFinish = ";";
        var isExtensionMethod = this.methodBase.IsDefined(typeof(ExtensionAttribute), true);
        var customInvocation = false;
        if (!isExtensionMethod)
        {
            if (this.methodBase.IsSpecialName)
            {
                var name = methodBase.Name;
                if (name.StartsWith("add_"))
                {
                    if (this.methodBase.IsStatic) sb.Append(Utils.GetTypeNameForNaming(this.methodBase.DeclaringType));
                    else sb.Append(paramNames[instanceArgName]);
                    sb.Append(".");
                    sb.Append(this.methodBase.Name.Substring("add_".Length));
                    sb.Append(" += ");
                }
                else if (name.StartsWith("remove_"))
                {
                    if (this.methodBase.IsStatic) sb.Append(Utils.GetTypeNameForNaming(this.methodBase.DeclaringType));
                    else sb.Append(paramNames[instanceArgName]);
                    sb.Append(".");
                    sb.Append(this.methodBase.Name.Substring("remove_".Length));
                    sb.Append(" -= ");
                }
                else if (name.StartsWith("get_"))
                {
                    var methodName = this.methodBase.Name.Substring("get_".Length);
                    if (this.methodBase.IsStatic) sb.Append(Utils.GetTypeNameForNaming(this.methodBase.DeclaringType));
                    else sb.Append(paramNames[instanceArgName]);

                    if (this.parameterInfos.Length == 0)
                    {
                        sb.Append(".");
                        sb.Append(methodName);
                    }
                    else // indexed prop
                    {
                        sb.Append("[");
                        lineFinish = "];";
                    }

                }
                else if (name.StartsWith("set_"))
                {
                    var methodName = this.methodBase.Name.Substring("set_".Length);
                    if (this.methodBase.IsStatic) sb.Append(Utils.GetTypeNameForNaming(this.methodBase.DeclaringType));
                    else sb.Append(paramNames[instanceArgName]);
                    
                    if (this.parameterInfos.Length == 1)
                    {
                        sb.Append(".");
                        sb.Append(methodName);
                        sb.Append(" = ");
                    }
                    else
                    {
                        sb.Append("[");
                        customInvocation = true;
                        sb.Append(paramNames[this.parameterInfos[0].Name]);
                        sb.Append("] = ");
                        sb.Append(paramNames[this.parameterInfos[1].Name]);
                        lineFinish = ";";
                    }
                }
                else if (this.methodBase.IsConstructor)
                {
                    sb.Append("new ");
                    sb.Append(typeWriter.LookupTypeAsText(this.methodBase.DeclaringType));
                    lineFinish = ");";
                    sb.Append("(");
                }
                else if (name.StartsWith("op_Equality"))
                {
                    customInvocation = true;
                    sb.Append(paramNames[this.parameterInfos[0].Name]);
                    sb.Append(" == ");
                    sb.Append(paramNames[this.parameterInfos[1].Name]);
                }
                else if (name.StartsWith("op_Inequality"))
                {
                    customInvocation = true;
                    sb.Append(paramNames[this.parameterInfos[0].Name]);
                    sb.Append(" != ");
                    sb.Append(paramNames[this.parameterInfos[1].Name]);
                }
                else
                {
                    throw new NotSupportedMethodException(this.methodWrittenDetails.EntryPoint);
                }
            }
            else
            {
                if (this.methodBase.IsStatic)
                {
                    sb.Append(this.methodBase.DeclaringType.Name);
                    sb.Append(".");
                }
                else if (instanceArgName != null)
                {
                    sb.Append(paramNames[instanceArgName]);
                    sb.Append(".");
                }

                if (this.fieldInfo == null)
                {

                    sb.Append(this.methodBase.Name);

                    lineFinish = ");";
                    sb.Append("(");
                }
                else
                {
                    sb.Append(this.fieldInfo.Name);
                    if (methodBase.Name.StartsWith("set_")) sb.Append(" = ");
                    lineFinish = ";";
                }
            }
        }

        if (!customInvocation)
        {
            var doComma = false;
            var first = true;
            foreach (var parameterInfo in this.parameterInfos)
            {
                if (first)
                {
                    first = false;
                    if (isExtensionMethod)
                    {
                        sb.Append(paramNames[parameterInfo.Name]);
                        sb.Append(".");
                        sb.Append(this.methodBase.Name);
                        lineFinish = ");";
                        sb.Append("(");
                        continue;
                    }
                }

                if (doComma) sb.Append(", ");
                if (parameterInfo.IsOut)
                {
                    sb.Append("out var ");
                }
                sb.Append(paramNames[parameterInfo.Name]);
                doComma = true;
            }
        }

        sb.Append(lineFinish);
        writer.Write(sb.ToString());
        
        foreach (var parameterInfo in this.parameterInfos)
        {
            if (parameterInfo.IsOut)
            {
                var result = WriteTypeConversion(writer, preFunctionWriter, parameterInfo.ParameterType, true, paramNames[parameterInfo.Name]);
                writer.Write($"Marshal.WriteIntPtr({parameterInfo.Name}, {result});");
            }
        }
        
        
        if (!string.IsNullOrWhiteSpace(resultName))
        {
            var result = WriteTypeConversion(writer, preFunctionWriter, this.returnType, true, resultName);
            writer.Write($"return {result};");
        }
    }

    private string WriteDefaultValue(DelayedWriter writer, ParameterInfo parameterInfo, string variableName)
    {
        if (!parameterInfo.IsOptional) return variableName;
        if (!parameterInfo.HasDefaultValue) return variableName;
        if (parameterInfo.ParameterType == typeof(bool)) return variableName; // this is handled in function header properly
        if (parameterInfo.DefaultValue == null) return variableName; // there is nothing to default to
        if (parameterInfo.IsOptional && Utils.IsEnum(parameterInfo.ParameterType)) return variableName;

        var sb = new StringBuilder();

        sb.Append(variableName);
        sb.Append(" = ");
        sb.Append(variableName);
        sb.Append(" != default ? ");
        sb.Append(variableName);
        sb.Append(" : ");
        if (parameterInfo.DefaultValue.GetType() == typeof(string))
        {
            sb.Append("\"");
            sb.Append(parameterInfo.DefaultValue);
            sb.Append("\"");
        } else if (parameterInfo.DefaultValue is Enum)
        {
            sb.Append(parameterInfo.DefaultValue.GetType().Name);
            sb.Append(".");
            sb.Append(parameterInfo.DefaultValue);
        }
        else
        {
            sb.Append(parameterInfo.DefaultValue);
        }

        sb.Append(";");
        writer.Write(sb.ToString());
        return variableName;
    }

    /// <summary>
    /// Creates C# code to convert the type between managed and unmanaged
    /// </summary>
    /// <param name="writer"></param>
    /// <param name="preFunctionWriter">writer to add something before the function that contains this conversion</param>
    /// <param name="typeToConvert"></param>
    /// <param name="toUnmanaged"></param>
    /// <param name="sourceName"></param>
    /// <param name="targetName"></param>
    /// <param name="createTargetVar"></param>
    /// <returns>The name of the variable the converted result will be assigned to</returns>
    /// <exception cref="Exception"></exception>
    /// <exception cref="NotSupportedMethodException"></exception>
    private string WriteTypeConversion(DelayedWriter writer, DelayedWriter preFunctionWriter, Type typeToConvert, bool toUnmanaged, string sourceName, string targetName = null, bool createTargetVar = true)
    {
        Type source, target;
        if (toUnmanaged)
        {
            source = typeToConvert;
            target = Utils.GetUnmanagedType(typeToConvert);
        }
        else
        {
            source = Utils.GetUnmanagedType(typeToConvert);
            target = typeToConvert;
        }
        
        sourceName = GetValidParameterName(sourceName); // none of the generated ones should be a reserved keyword, and if it is a parameter of a method, then it'll be the result of this method anyway
        var targetVarCreate = (createTargetVar ? "var " : "");
        if (target == source) return sourceName;

        if (source == typeof(IntPtr))
        {
            if (Utils.GetReplacementType(target) == typeof(string))
            {
                targetName ??= $"{sourceName}Str";
                writer.Write($"{targetVarCreate}{targetName} = {nameof(InteropUtils)}.{nameof(InteropUtils.PtrToStringUTF8)}({sourceName});");
                if (target == typeof(Span<char>)) // Span isn't string equatable, we need to convert
                {
                    var readSpanTargetName = $"{targetName}ReadSpan";
                    writer.Write($"var {readSpanTargetName} = {targetName}.AsSpan();");
                    var spanTargetName = $"{targetName}Span";
                    writer.Write($"var {spanTargetName} = new Span<char>(GC.AllocateUninitializedArray<char>({readSpanTargetName}.Length));");
                    writer.Write($"{readSpanTargetName}.CopyTo({spanTargetName});");
                    targetName = spanTargetName;
                }

                return targetName;
            }
            
            if (!toUnmanaged && Utils.IsBlittableType(target))
            {
                targetName ??= $"{sourceName}UPtr";
                writer.Write($"{targetVarCreate}{targetName} = {nameof(InteropUtils)}.{nameof(InteropUtils.FromUPtr)}<{target.Name}>({sourceName});");
                return targetName;
            }

            if (typeof(IDictionary).IsAssignableFrom(target))
            {
                targetName ??= $"{sourceName}Dict";
                var dictTypeText = typeWriter.LookupTypeAsText(target);
                var generics = target.GetGenericArguments();
                if (generics.Length == 0)
                {
                    var firstKeyValuePairType = target.GetInterfaces().Select(y=> y.GetGenericArguments()
                        .FirstOrDefault(x=> x.IsGenericType  && x.GetGenericTypeDefinition() == typeof(KeyValuePair<,>))).FirstOrDefault(y=> y!= null);
                    if (firstKeyValuePairType == null) throw new Exception($"Unable to look up Dictionary generic type for {target}");
                    generics = firstKeyValuePairType.GetGenericArguments();

                }
                var keyTypeText = typeWriter.LookupTypeAsText(generics[0]);
                var valueTypeText = typeWriter.LookupTypeAsText(generics[1]);
                writer.Write($"{targetVarCreate}{targetName} = {nameof(DictionaryInterop)}.{nameof(DictionaryInterop.FromUPtr)}<{dictTypeText}, {keyTypeText}, {valueTypeText}>({sourceName});");
                return targetName;
            }
            
            if (source != typeof(Array) && typeof(Array).IsAssignableFrom(target))
            {
                // The following is using array allocation with size rather than linq in order to improve performance and
                // remove the need for referencing LINQ. Resulting code is an eyesore, but better overall
                var unmanagedArrayName = $"{sourceName}ArrUnmanaged";
                var generic = target.GetElementType();
                var elementTypeAsText = typeWriter.LookupTypeAsText(generic);
                var elementTypeAsUnmanagedText = typeWriter.GetInteropTypeString(generic, false);
                writer.Write($"var {unmanagedArrayName} = {nameof(InteropUtils)}.{nameof(InteropUtils.FromArrayUPtr)}({sourceName}, typeof({elementTypeAsUnmanagedText})) as {elementTypeAsUnmanagedText}[];");
                targetName ??= $"{sourceName}Arr";
                writer.Write($"{targetVarCreate}{targetName} = {unmanagedArrayName} == null ? null : new {elementTypeAsText}[{unmanagedArrayName}.Length];");
                writer.Write($"if ({targetName} != null) {{"); // start of null check
                writer.IncrementIndent();
                writer.Write($"for (var {targetName}Index = 0; {targetName}Index < {targetName}.Length; {targetName}Index++) {{"); // start of for
                writer.IncrementIndent();
                var innerBodyConvertResultName = WriteTypeConversion(writer, preFunctionWriter, generic, false, $"{unmanagedArrayName}[{targetName}Index]", $"{targetName}Converted");
                writer.Write($"{targetName}[{targetName}Index] = {innerBodyConvertResultName};");
                writer.DecrementIndent();
                writer.Write("}"); // end of for
                writer.DecrementIndent();
                writer.Write($"}}"); // end of null check
                return targetName;
            }
           
            targetName ??= $"{sourceName}Inst";
            
            var typeText = typeWriter.LookupTypeAsText(target);

            writer.Write($"{targetVarCreate}{targetName} = {nameof(InteropUtils)}.{nameof(InteropUtils.FromHPtr)}<{typeText}>({sourceName});");
            return targetName;
        }
        
        if (target == typeof(IntPtr))
        {
            if (Utils.GetReplacementType(source) == typeof(string))
            {
                targetName ??= $"{sourceName}UPtr";
                writer.Write($"{targetVarCreate}{targetName} = {nameof(InteropUtils)}.{nameof(InteropUtils.Utf8StringToUPtr)}({sourceName});");
                return targetName;
            }

            if (toUnmanaged && Utils.IsBlittableType(source))
            {
                targetName ??= $"{sourceName}UPtr";
                writer.Write($"{targetVarCreate}{targetName} = {nameof(InteropUtils)}.{nameof(InteropUtils.ToUPtr)}({sourceName});");
                return targetName;
            }

            if (source != typeof(Array) && typeof(Array).IsAssignableFrom(source))
            {
                // The following is using array allocation with size rather than linq in order to improve performance and
                // remove the need for referencing LINQ. Resulting code is an eyesore, but better overall
                targetName ??= $"{sourceName}ArrUnmanaged";
                var generic = source.GetElementType();
                var elementTypeAsText = typeWriter.LookupTypeAsText(generic);
                if (generic == typeof(string))
                {
                    writer.Write($"var {targetName} = {nameof(CollectionInterop)}.{nameof(CollectionInterop.ToUPtrString)}({sourceName});");
                }
                else
                {
                    writer.Write($"var {targetName} = {nameof(InteropUtils)}.{nameof(InteropUtils.ToArrayUPtr)}({sourceName}, typeof({elementTypeAsText}));");
                }

                return targetName;
            }

            targetName ??= $"{sourceName}HPtr";
            writer.Write($"{targetVarCreate}{targetName} = {nameof(InteropUtils)}.{nameof(InteropUtils.ToHPtr)}({sourceName});");
            return targetName;
        }

        if (typeof(Delegate).IsAssignableFrom(target) && source.BaseType == typeof(MulticastDelegate))
        {
            var delegateMethodInfo = Utils.GetMulticastDelegateMethodInfo(source);
            targetName = $"{sourceName}Unmanaged";
            var headerSb = new StringBuilder();
            headerSb.Append(typeWriter.GetInteropTypeString(delegateMethodInfo.ReturnType, true));
            headerSb.Append(" ");
            headerSb.Append(targetName);
            headerSb.Append("(");
            var delegateParameters = delegateMethodInfo.GetParameters();
            for (var index = 0; index < delegateParameters.Length; index++)
            {
                var parameterInfo = delegateParameters[index];
                if (index != 0) headerSb.Append(", ");
                headerSb.Append(typeWriter.GetInteropTypeString(parameterInfo.ParameterType));
                headerSb.Append(" ");
                var name = $"arg{index}";
                headerSb.Append(name);
            }

            headerSb.Append(")");
            writer.Write(headerSb.ToString());
            writer.Write("{");
            writer.IncrementIndent();
            
            var invocationSb = new StringBuilder();
            invocationSb.Append(sourceName);
            invocationSb.Append("(");
            
            if (delegateParameters.Length == 0)
            {
                invocationSb.Append(");");
                writer.Write(invocationSb.ToString());
            }
            else
            {
                var appendEmptyLineAfterConversions = false;

                for (var index = 0; index < delegateParameters.Length; index++)
                {
                    var parameterInfo = delegateParameters[index];
                    if (index != 0) invocationSb.Append(", ");
                    var name = $"arg{index}";
                    var newName = WriteTypeConversion(writer, preFunctionWriter, parameterInfo.ParameterType, false, name);
                    appendEmptyLineAfterConversions |= name != newName;
                    invocationSb.Append(newName);
                }

                var hasReturn = delegateMethodInfo.ReturnType != typeof(void);
                if (appendEmptyLineAfterConversions) writer.WriteEmptyLine();
                invocationSb.Append(");");
                if (hasReturn) invocationSb.Insert(0, $"var {targetName}Result = ");
                writer.Write(invocationSb.ToString());
                if (hasReturn)
                {
                    var newName = WriteTypeConversion(writer, preFunctionWriter, delegateMethodInfo.ReturnType, true, $"{targetName}Result");
                    writer.Write("return " + newName + ";");
                }
            }

            writer.DecrementIndent();
            writer.Write("}");

            // With .NET8 this functions is necessary to avoid runtime exception regarding generics with Marshal.GetFunctionPointerForDelegate
            var delegateName = $"{this.methodWrittenDetails.UniqueMethodName}_{targetName}";
            preFunctionWriter.Write($"delegate {headerSb.ToString().Replace(targetName, delegateName)};");

            var targetNameDelegate = $"{targetName}Delegate";
            writer.Write($"{delegateName} {targetNameDelegate} = {targetName};");
            return $"Marshal.GetFunctionPointerForDelegate({targetNameDelegate})";
        }


        if (typeof(Delegate).IsAssignableFrom(source) &&  target.BaseType == typeof(MulticastDelegate))
        {
            targetName ??= target.IsGenericType
                ? $"{sourceName}{target.Name.Split('`')[0]}"
                : $"{sourceName}{target.Name}";
            
            void WriteDelegateHeader(Type[] arguments, bool hasReturn)
            {
                //writer.Write($"Console.WriteLine(\"{this.methodWrittenDetails.EntryPoint} function ptr: \" + new IntPtr({sourceName}));"); // helpful for debug
                if (arguments.Length == 0)
                {
                    writer.Write($"{typeWriter.LookupTypeAsText(target)} {targetName} = {sourceName} == null ? default : () => {{");
                }
                else
                {
                    var sb = new StringBuilder();
                    sb.Append(typeWriter.LookupTypeAsText(target));
                    sb.Append(" ");
                    sb.Append(targetName);
                    sb.Append($" = {sourceName} == null ? default : (");
                    for (var index = 0; index < arguments.Length; index++)
                    {
                        if (index != arguments.Length -1 || !hasReturn)
                        {
                            if (index > 0) sb.Append(", ");
                            var name = $"arg{index}";
                            sb.Append(name);
                        }
                    }

                    sb.Append(") => {");
                    writer.Write(sb.ToString());
                    //writer.Write($"Console.WriteLine(\"{this.methodWrittenDetails.EntryPoint} invoking with func ptr: \" + new IntPtr({sourceName}));"); // helpful for debug
                } 
            }
            
            void WriteDelegateBody(Type[] arguments, bool hasReturn)
            {
                var invocationSb = new StringBuilder();
                invocationSb.Append(sourceName);
                invocationSb.Append("(");
                if (arguments.Length == 0)
                {
                    invocationSb.Append(");");
                    writer.Write(invocationSb.ToString());
                    return;
                }

                var appendEmptyLineAfterConversions = false;

                for (var index = 0; index < arguments.Length; index++)
                {
                    var type = arguments[index];
                    if (index != arguments.Length - 1 || !hasReturn)
                    {
                        if (index != 0) invocationSb.Append(", ");
                        var name = $"arg{index}";
                        var result = WriteTypeConversion(writer, preFunctionWriter, type, true, name);
                        //if (unmanagedType == typeof(IntPtr)) writer.Write($"Console.WriteLine(\"{this.methodWrittenDetails.EntryPoint} invoking with value ptr: \" + {result});"); // helpful for debug
                        //else writer.Write($"Console.WriteLine(\"{this.methodWrittenDetails.EntryPoint} invoking with value: \" + {result});"); // helpful for debug
                        appendEmptyLineAfterConversions |= name != result;
                        invocationSb.Append(result);
                    }
                
                    if (index == arguments.Length - 1)
                    {
                        if (appendEmptyLineAfterConversions) writer.WriteEmptyLine();
                        writer.Write($"InteropUtils.LogDebug($\"Invoking handler {{(IntPtr){sourceName}}} for {this.methodWrittenDetails.EntryPoint}\");");
                        invocationSb.Append(");");
                        if (hasReturn) invocationSb.Insert(0, "var result = ");
                        writer.Write(invocationSb.ToString());
                        if (hasReturn)
                        {
                            var result = WriteTypeConversion(writer, preFunctionWriter, type, false, "result");
                            writer.Write("return " + result + ";");
                        }
                    }
                }   
            }

            var typeAsString = target.IsGenericType
                ? $"{sourceName}{target.Name.Split('`')[0]}"
                : $"{sourceName}{target.Name}";
            
            if (typeAsString.EndsWith("Action"))
            {
                writer.Write($"InteropUtils.LogDebug($\"Registering action handler {{(IntPtr){sourceName}}} for {this.methodWrittenDetails.EntryPoint}\");");
                WriteDelegateHeader(target.GenericTypeArguments, false);
                writer.IncrementIndent();
                WriteDelegateBody(target.GenericTypeArguments, false);
            }
            else if (typeAsString.EndsWith("Func"))
            {
                writer.Write($"InteropUtils.LogDebug($\"Registering func handler {{(IntPtr){sourceName}}} for {this.methodWrittenDetails.EntryPoint}\");");
                WriteDelegateHeader(target.GenericTypeArguments, true);
                writer.IncrementIndent();
                WriteDelegateBody(target.GenericTypeArguments, true);
            }
            else if (typeAsString.EndsWith("EventHandler"))
            {
                var types = target.GenericTypeArguments;
                if (types.Length == 0)
                {
                    types = new Type[]
                    {
                        typeof(object),
                        typeof(EventArgs)
                    };
                } else if (types.Length == 1)
                {
                    types = new Type[]
                    {
                        typeof(object),
                        types[0]
                    };
                }
                writer.Write($"InteropUtils.LogDebug($\"Registering event handler {{(IntPtr){sourceName}}} for {this.methodWrittenDetails.EntryPoint}\");");
                WriteDelegateHeader(types, false);
                writer.IncrementIndent();
                WriteDelegateBody(types, false);
            }
            else throw new NotSupportedMethodException("Unhandled type");
            writer.DecrementIndent();
            writer.Write("};");
            return targetName;
        }

        if (target == typeof(byte))
        {
            targetName ??= $"{sourceName}Byte";
            writer.Write($"{targetVarCreate}{targetName} = Convert.ToByte({sourceName});");
            return targetName;
        }
        if (target == typeof(bool))
        {
            targetName ??= $"{sourceName}Boolean";
            writer.Write($"{targetVarCreate}{targetName} = Convert.ToBoolean({sourceName});");
            return targetName;
        }

        return sourceName;
    }

    private Dictionary<string, string> WriteFunctionHeader(DelayedWriter writer)
    {
        var args = new Dictionary<string, string>();
        writer.Write("[UnmanagedCallersOnly(EntryPoint = \"" + this.methodWrittenDetails.EntryPoint + "\")]");
        
        var sb = new StringBuilder();
        sb.Append("public static ");
        if (IsUnsafeMethod()) sb.Append("unsafe ");
        sb.Append(typeWriter.GetInteropTypeString(this.returnType, true));
        sb.Append(" ");
        sb.Append(this.methodWrittenDetails.UniqueMethodName);
        sb.Append("(");

        var first = true;
        var instanceParameterName = Utils.GetInstanceParameterName(this.methodWrittenDetails.DeclaringType, this.methodBase);
        if (instanceParameterName != null)
        {
            first = false;
            var typeString = typeWriter.GetInteropTypeString(this.methodBase.DeclaringType);
            sb.Append(typeString);
            sb.Append(" ");
            var pName = GetValidParameterName(instanceParameterName);
            sb.Append(pName);
            args.Add(pName, typeString);
        }
        
        foreach (var parameterInfo in this.methodParameterOrder)
        {
            if (!first) sb.Append(", ");
            first = false;
            var typeString = typeWriter.GetInteropTypeString(parameterInfo.ParameterType);
            sb.Append(typeString);
            sb.Append(" ");
            var pName = GetValidParameterName(parameterInfo.Name);
            sb.Append(pName);
            args.Add(pName, typeString);
            if (parameterInfo.IsOptional)
            {
                sb.Append(" = ");
                if (!parameterInfo.HasDefaultValue)
                {
                    sb.Append("default");
                }
                else
                {
                    if (parameterInfo.ParameterType == typeof(bool)) sb.Append("1"); // because we convert it to byte
                    else if (Utils.IsEnum(parameterInfo.ParameterType))
                    {
                        sb.Append(parameterInfo.DefaultValue.GetType().Name);
                        sb.Append(".");
                        sb.Append(parameterInfo.DefaultValue);
                    } 
                    else sb.Append("default");
                }
            }
        }

        sb.Append(")");
        writer.Write(sb.ToString());

        return args;
    }

    private static string GetValidParameterName(string actual)
    {
        if (!CsharpUtils.IsReservedWord(actual)) return actual;
        return "@" + actual;
    }

    private bool IsUnsafeMethod()
    {
        return Utils.GetUsedTypes(this.methodWrittenDetails.DeclaringType, this.methodBase).Any(Utils.IsDelegate);
    }

    
    private string GetMethodName()
    {
        var name = methodBase.Name;
        return (name == ".ctor" ? "Constructor" : name) + nameSuffix;
    }

    private string GetEntryPointName()
    {
        var nameBase = Utils.GetTypeNameForNaming(this.methodWrittenDetails.DeclaringType).ToLowerInvariant();
        var methodName = this.methodWrittenDetails.UniqueMethodName.ToLowerInvariant();
        return $"{nameBase}_{methodName}";
    }
}
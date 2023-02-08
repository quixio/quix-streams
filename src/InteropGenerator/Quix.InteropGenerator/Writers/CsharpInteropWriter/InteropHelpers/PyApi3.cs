using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;

namespace InteropHelpers.Interop;

internal static class DllLoader
{
    public static IntPtr GetSymbol(IntPtr dll, string symbolName)
    {
        var res = NativeLibrary.GetExport(dll, symbolName);
        Console.WriteLine($"Loading Symbol in {dll} with name {symbolName} and value {res}");
        if (res == IntPtr.Zero) throw new MissingMethodException($"Symbol {symbolName} was not found");
        return res;
    }
    
    public static IntPtr LoadDll(string libName)
    {
        // According to https://learn.microsoft.com/en-us/dotnet/standard/native-interop/cross-platform
        // and https://learn.microsoft.com/en-us/dotnet/standard/native-interop/pinvoke-source-generation
        // This should also attempt to load the correct library on linux and macos
        Console.WriteLine($"Loading {libName}");
        var dll = NativeLibrary.Load(libName);
        if (dll == IntPtr.Zero) throw new DllNotFoundException($"{libName} could not be loaded");
        return dll;
    }
    
    public static void Free(IntPtr dll)
    {
        if (dll == IntPtr.Zero) return;
        NativeLibrary.Free(dll);
    }
    
    public static bool TryGetPythonLibraryPath(string pythonLib, out string pythonLibPath)
    {
        var extension = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? "dll" :
            RuntimeInformation.IsOSPlatform(OSPlatform.Linux) ? "so" : "dylib";
        pythonLibPath = null;
        var envVariables = Environment.GetEnvironmentVariables().Cast<DictionaryEntry>()
            .ToDictionary(x => (string)x.Key, x => (string)x.Value);

        var path = envVariables.FirstOrDefault(y => y.Key.Equals("path", StringComparison.InvariantCultureIgnoreCase))
            .Value;
        if (string.IsNullOrWhiteSpace(path)) return false;
        var folders = path.Split(Path.PathSeparator);
        foreach (var folder in folders)
        {
            if (TryGetPythonLibFromFolder(folder, pythonLib, extension, out pythonLibPath))
            {
                return true;
            }
        }

        var libFolders = new List<string>();
        if (!RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
        {
            Console.WriteLine($"Checking linux folders");
            libFolders.Add("/usr/lib/");
            libFolders.Add("/usr/local/lib/");
            if (envVariables.TryGetValue("HOME", out var homeEnvVar)) libFolders.Add(Path.Combine(homeEnvVar, ".local", "lib"));
        }

        if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
        {
            Console.WriteLine($"Checking mac folders");
            // x64_86 /usr/local/Cellar/python@3.10/3.10.9/Frameworks/Python.framework/Versions/3.10/lib
            if (RuntimeInformation.OSArchitecture == Architecture.X64)
            {
                const string x64dir = "/usr/local/Cellar";

                void SetX64() // for early returns
                {
                    if (!Directory.Exists(x64dir)) return;
                    var dirs = Directory.GetDirectories(x64dir).Where(y => y.StartsWith("python@3")).ToList();
                    dirs.ForEach(x=> Console.WriteLine($"x64-1: {x}"));
                    if (!dirs.Any()) return;
                    var frameworkDirs = dirs.SelectMany(Directory.GetDirectories)
                        .Select(x => Path.Combine(x, "Frameworks"))
                        .Where(Directory.Exists).ToList();
                    frameworkDirs.ForEach(x=> Console.WriteLine($"x64-2: {x}"));
                    if (!frameworkDirs.Any()) return;
                    var versions = frameworkDirs.Select(x => Path.Combine(x, "Python.framework", "Versions"))
                        .SelectMany(Directory.GetDirectories).Select(x=> Path.Combine(x, "lib")).Where(Directory.Exists).ToList();
                    versions.ForEach(x=> Console.WriteLine($"x64-3: {x}"));
                    if (!versions.Any()) return;
                    libFolders.AddRange(versions);
                }
                SetX64();
            }
            
            // universal /Library/Frameworks/Python.framework/Versions/Current/lib/python3.11/config-3.11-darwin/libpython3.11.dylib
            const string universal = "/Library/Frameworks/Python.framework/Versions/Current/lib/";
            void SetUniversal() // for early returns 
            {
                if (!Directory.Exists(universal)) return;
                Console.WriteLine($"uni-1: {universal}");
                var possibleUniversalDirs = Directory.GetDirectories(universal).Where(y => y.StartsWith("python3"))
                    .SelectMany(Directory.GetDirectories).Where(y => y.StartsWith("config-") && y.EndsWith("-darwin")).ToList();
                possibleUniversalDirs.ForEach(x=> Console.WriteLine($"uni-2: {x}"));
                if (!possibleUniversalDirs.Any()) return;
                libFolders.AddRange(possibleUniversalDirs);
            }
        }

        foreach (var folder in libFolders)
        {
            if (TryGetPythonLibFromFolder(folder, pythonLib, extension, out pythonLibPath))
            {
                return true;
            }
        }
        
        
        return false;
    }

    private static bool TryGetPythonLibFromFolder(string folder, string fileStart, string extension, out string pythonLibPath)
    {
        if (!Directory.Exists(folder))
        {
            pythonLibPath = null;
            return false;
        }
        var files = Directory.GetFiles(folder);
        var matchingFile = files.FirstOrDefault(y =>
        {
            var fileName = Path.GetFileName(y);
            return fileName.StartsWith(fileStart, StringComparison.InvariantCultureIgnoreCase) &&
                   fileName.EndsWith($".{extension}", StringComparison.InvariantCultureIgnoreCase);
        });
        if (!string.IsNullOrWhiteSpace(matchingFile))
        {
            pythonLibPath = matchingFile;
            return true;
        }

        var dirs = Directory.GetDirectories(folder).Where(y=>
        {
            var folder = Path.GetFileName(y);
            return folder.StartsWith("python") || folder.EndsWith("-gnu");
        });
        foreach (var dir in dirs)
        {
            if (TryGetPythonLibFromFolder(dir,  fileStart, extension, out pythonLibPath))
            {
                return true;
            }
        }

        pythonLibPath = null;
        return false;
    }
    
}


public class PyApi3 : IDisposable
{
    private unsafe delegate* unmanaged<void> Py_Initialize;
    private unsafe delegate* unmanaged<int> Py_IsInitialized;
    private unsafe delegate* unmanaged<IntPtr> Py_GetVersion;
    
    private unsafe delegate* unmanaged<void> PyErr_NoMemory; // PyObject *PyErr_NoMemory()
    private unsafe delegate* unmanaged<IntPtr, IntPtr, void> PyErr_SetString; // void PyErr_SetString(PyObject *type, const char *message)
    private unsafe delegate* unmanaged<IntPtr, IntPtr> PyImport_ImportModule; // PyObject *PyImport_ImportModule(const char *name)
    
    
    private unsafe delegate* unmanaged<IntPtr, IntPtr, IntPtr> PyObject_GetAttr; // PyObject *PyObject_GetAttr(PyObject *o, PyObject *attr_name)
    private unsafe delegate* unmanaged<IntPtr, IntPtr, IntPtr> PyObject_GetAttrString; // PyObject *PyObject_GetAttrString(PyObject *o, const char *attr_name)

    private unsafe delegate* unmanaged<int> PyGILState_Check; // int PyGILState_Check()
    private unsafe delegate* unmanaged<int> PyGILState_Ensure; // PyGILState_STATE PyGILState_Ensure()
    private unsafe delegate* unmanaged<int, void> PyGILState_Release; // void PyGILState_Release(PyGILState_STATE)


    #region Exceptions
    private IntPtr exception;
    private IntPtr notImplementedError;
    #endregion

    #region library pointers
    private IntPtr baseLibPtr;
    private IntPtr versionedLibPtr;
    #endregion
    
    public Version Version { get; private set; }

    public PyApi3()
    {
        LoadBase();
        LoadVersioned();
    }

    private void LoadBase()
    {
        var libName = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? $"python3" : $"libpython3";
        try
        {
            this.baseLibPtr = DllLoader.LoadDll(libName);
        }
        catch (DllNotFoundException)
        {
            if (DllLoader.TryGetPythonLibraryPath(libName, out var pythonLibPath))
            {
                this.baseLibPtr = DllLoader.LoadDll(pythonLibPath);
            }
            else
            {
                throw;
            }
        }

        LoadBaseFunctions();
        SetVersion();
    }
    
    private void SetVersion()
    {
        var version = GetVersion().Split(' ')[0];
        var segments = version.Split('.');
        var major = segments[0];
        var minor = segments[1];
        string micro = "0";
        if (segments.Length > 2)
        {
            micro = version.Split('.')[2];
        }

        this.Version = new Version(Int32.Parse(major), Int32.Parse(minor), Int32.Parse(micro));
    }

    private void LoadVersioned()
    {
        var libName = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? $"python3{Version.Minor}" : $"libpython3.{Version.Minor}";
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            try
            {
                this.versionedLibPtr = DllLoader.LoadDll(libName);
            }
            catch (DllNotFoundException)
            {
                if (DllLoader.TryGetPythonLibraryPath(libName, out var pythonLibPath))
                {
                    this.versionedLibPtr = DllLoader.LoadDll(pythonLibPath);
                }
                else
                {
                    throw;
                }
            }
        }
        else
        {
            this.versionedLibPtr = baseLibPtr;
        }

        LoadVersionedFunctions();
    }
    
    private unsafe void LoadBaseFunctions()
    {
        Py_IsInitialized = (delegate* unmanaged<int>)DllLoader.GetSymbol(baseLibPtr, nameof(Py_IsInitialized));
        Py_Initialize = (delegate* unmanaged<void>)DllLoader.GetSymbol(baseLibPtr, nameof(Py_Initialize));
        Py_GetVersion = (delegate* unmanaged<IntPtr>)DllLoader.GetSymbol(baseLibPtr, nameof(Py_GetVersion));
    }

    private unsafe void LoadVersionedFunctions()
    {
        Console.WriteLine("A");
        PyGILState_Check = (delegate* unmanaged<int>)DllLoader.GetSymbol(this.versionedLibPtr, nameof(PyGILState_Check));
        Console.WriteLine("B");
        PyGILState_Ensure = (delegate* unmanaged<int>)DllLoader.GetSymbol(this.versionedLibPtr, nameof(PyGILState_Ensure));
        Console.WriteLine("C");
        PyGILState_Release = (delegate* unmanaged<int, void>)DllLoader.GetSymbol(this.versionedLibPtr, nameof(PyGILState_Release));
        Console.WriteLine("D");
        PyImport_ImportModule = (delegate* unmanaged<IntPtr, IntPtr>)DllLoader.GetSymbol(this.versionedLibPtr, nameof(PyImport_ImportModule));
        Console.WriteLine("E");
        PyObject_GetAttr = (delegate* unmanaged<IntPtr, IntPtr, IntPtr>)DllLoader.GetSymbol(this.versionedLibPtr, nameof(PyObject_GetAttr));
        Console.WriteLine("F");
        PyObject_GetAttrString = (delegate* unmanaged<IntPtr, IntPtr, IntPtr>)DllLoader.GetSymbol(this.versionedLibPtr, nameof(PyObject_GetAttrString));
        Console.WriteLine("G");
        
        PyErr_NoMemory = (delegate* unmanaged<void>)DllLoader.GetSymbol(this.versionedLibPtr, nameof(PyErr_NoMemory));
        Console.WriteLine("H");
        PyErr_SetString = (delegate* unmanaged<IntPtr, IntPtr, void>)DllLoader.GetSymbol(this.versionedLibPtr, nameof(PyErr_SetString));
        Console.WriteLine("J");
        
        Initialize();
        Console.WriteLine($"K {IsInitialized()}");
        using var GILState = EnsureGILState();
        Console.WriteLine($"L {CheckGILState()}");
        IntPtr builtins_module = ImportModule("builtins");
        Console.WriteLine("M");
        this.exception = GetAttributeString(builtins_module, "Exception");
        Console.WriteLine("N");
        this.notImplementedError = GetAttributeString(builtins_module, "NotImplementedError");
        Console.WriteLine("P");
    }
    
    public unsafe bool CheckGILState()
    {
        return PyGILState_Check() == 1;
    }
    
    public unsafe IDisposable EnsureGILState()
    {
        return new PythonGILLock(this, PyGILState_Ensure());
    }
    
    private unsafe void ReleaseGILState(int state)
    {
        PyGILState_Release(state);
    }

    public unsafe IntPtr ImportModule(string moduleName)
    {
        Console.WriteLine("ZZ");
        var moduleNamePtr = InteropUtils.Utf8StringToUPtr(moduleName);
        Console.WriteLine($"ZZ2 {moduleName}");
        Console.WriteLine($"ZZ2 {(IntPtr)PyImport_ImportModule}");
        var modulePtr = PyImport_ImportModule(moduleNamePtr);
        Console.WriteLine($"ZZ3 {modulePtr}");
        return modulePtr;
    }
    
    public unsafe IntPtr GetAttribute(IntPtr objectPtr, string attributeName)
    {
        var attributeNamePtr = InteropUtils.Utf8StringToUPtr(attributeName);
        var attributePtr = PyObject_GetAttr(objectPtr, attributeNamePtr);
        return attributePtr;
    }
    
    public unsafe IntPtr GetAttributeString(IntPtr objectPtr, string attributeName)
    {
        var attributeNamePtr = InteropUtils.Utf8StringToUPtr(attributeName);
        var attributePtr = PyObject_GetAttrString(objectPtr, attributeNamePtr);
        return attributePtr;
    }

    public unsafe void RaiseException(Exception ex)
    {
        var exception = ex.ToString();
        var msgPtr = InteropUtils.Utf8StringToUPtr(exception);
        if (ex is NotImplementedException)
        {
            PyErr_SetString(this.notImplementedError, msgPtr);
            return;
        }
        PyErr_SetString(this.exception, msgPtr);
    }
    
    public unsafe void RaiseExceptionNoMemory()
    {
        PyErr_NoMemory();
    }


    public void Dispose()
    {
        var freeBoth = this.baseLibPtr != this.versionedLibPtr;
        DllLoader.Free(this.baseLibPtr);
        if (!freeBoth)
        {
            return;
        }
        DllLoader.Free(this.versionedLibPtr);
    }

    public unsafe string GetVersion()
    {
        var versionPtr = Py_GetVersion();
        return InteropUtils.PtrToStringUTF8(versionPtr);
    }

    public unsafe void Initialize()
    {
        if (IsInitialized()) return;
        Py_Initialize();
    }

    public unsafe bool IsInitialized()
    {
        return Py_IsInitialized() == 1;
    }
    
    public class PythonGILLock : IDisposable
    {
        private readonly PyApi3 api;
        public readonly int State;

        public PythonGILLock(PyApi3 api, int state)
        {
            this.api = api;
            this.State = state;
        }

        public void Dispose()
        {
            this.api.ReleaseGILState(this.State);
        }
    }
}
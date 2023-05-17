using System;
using System.Runtime.InteropServices;
using Microsoft.Extensions.Logging;
using Serilog;
using Serilog.Sinks.SystemConsole.Themes;

namespace Quix.InteropGenerator;

public static class Logger
{
    public static ILoggerFactory LoggerFactory = Microsoft.Extensions.Logging.LoggerFactory.Create(c =>
    {
        var minimumLogLevel = LogLevel.Trace; // TODO: bring it in from config
        c.ClearProviders();
        c.SetMinimumLevel(minimumLogLevel);

        
        var builder = new LoggerConfiguration()
            .Enrich.FromLogContext()
            .WriteTo.Console(theme: AnsiConsoleTheme.Literate, applyThemeToRedirectedOutput: true, outputTemplate: "[{Level:u3} ({SourceContext})] {Message:lj}{NewLine}{Exception}");
        switch (minimumLogLevel)
        {
            case LogLevel.Trace:
                builder.MinimumLevel.Verbose();
                break;
            case LogLevel.Debug:
                builder.MinimumLevel.Debug();
                break;
            case LogLevel.Information:
                builder.MinimumLevel.Information();
                break;
            case LogLevel.Warning:
                builder.MinimumLevel.Warning();
                break;
            case LogLevel.Error:
                builder.MinimumLevel.Error();
                break;
            case LogLevel.Critical:
                builder.MinimumLevel.Fatal();
                break;
            case LogLevel.None:
                builder.MinimumLevel.Fatal(); // there is no None, closest to it is this
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(minimumLogLevel), minimumLogLevel, null);
        }

        var logger = builder.CreateLogger();
        c.AddSerilog(logger, dispose:true);
                
        SerilogWindowsConsole.EnableVirtualTerminalProcessing();
    });
}

internal static class SerilogWindowsConsole
{
    public static void EnableVirtualTerminalProcessing()
    {
#if RUNTIME_INFORMATION
            if (!RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                return;
#else
        if (Environment.OSVersion.Platform != PlatformID.Win32NT)
            return;
#endif
        var stdout = GetStdHandle(StandardOutputHandleId);
        if (stdout != (IntPtr)InvalidHandleValue && GetConsoleMode(stdout, out var mode))
        {
            SetConsoleMode(stdout, mode | EnableVirtualTerminalProcessingMode);
        }
    }

    const int StandardOutputHandleId = -11;
    const uint EnableVirtualTerminalProcessingMode = 4;
    const long InvalidHandleValue = -1;

    [DllImport("kernel32.dll", SetLastError = true)]
    static extern IntPtr GetStdHandle(int handleId);

    [DllImport("kernel32.dll", SetLastError = true)]
    static extern bool GetConsoleMode(IntPtr handle, out uint mode);

    [DllImport("kernel32.dll", SetLastError = true)]
    static extern bool SetConsoleMode(IntPtr handle, uint mode);
}
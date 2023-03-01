using System;
using System.Runtime.InteropServices;

namespace QuixStreams
{
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

}
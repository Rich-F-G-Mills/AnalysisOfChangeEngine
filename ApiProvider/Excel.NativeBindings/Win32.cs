
namespace AnalysisOfChangeEngine.ApiProvider.Excel.NativeBindings
{
    using System.Runtime.InteropServices;


    // Here we delve into the murky depths of C# which, imperative nature
    // aside, is useful when needing managed wrappers around native calls.

    public static class Win32
    {
        [DllImport("oleacc.dll")]
        public static extern int AccessibleObjectFromWindow(
            [In] IntPtr hwnd,
            [In] uint dwId,
            // TODO - The underlying native type is REFIID... Is that why we're using ref Guid here?
            [In] ref Guid riid,
            [Out, MarshalAs(UnmanagedType.IUnknown)] out Object ppvObject
        );


        [DllImport("user32.dll", CharSet = CharSet.Auto)]
        public static extern int GetClassName(
            [In] IntPtr hWnd,
            // See "Fixed Length String Buffers" section of...
            // https://learn.microsoft.com/en-us/dotnet/framework/interop/default-marshalling-for-strings
            [Out] char[] lpClassName,
            [In] int nMaxCount
        );


        public delegate bool EnumWindowProc(IntPtr hwnd, IntPtr lParam);

        [DllImport("User32.dll")]
        public static extern bool EnumChildWindows(
            [In] IntPtr hWndParent,
            [In] EnumWindowProc lpEnumFunc,
            [In] IntPtr lParam
        );
    }
}


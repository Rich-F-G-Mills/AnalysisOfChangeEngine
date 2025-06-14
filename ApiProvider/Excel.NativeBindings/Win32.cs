
namespace AnalysisOfChangeEngine.ApiProvider.Excel.NativeBindings
{
    using System.Runtime.InteropServices;

    public static class Win32
    {
        [DllImport("oleacc.dll")]
        public static extern int AccessibleObjectFromWindow(
            [In] IntPtr hwnd,
            [In] uint dwId,
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



namespace AnalysisOfChangeEngine.ApiProvider.Excel


// Idiomatic F# wrappers around the native Win32 bindings provided by C#.
module internal Win32 =

    open System
    open System.Collections.Generic


    let internal accessibleObjectFromWindow targetId clsid hWnd =
        match NativeBindings.Win32.AccessibleObjectFromWindow (hWnd, targetId, ref clsid) with
        | HResultError err, _ -> Error err
        | _, obj -> Ok obj


    let internal getClassName hWnd =
        let nameBuffer =
            Array.zeroCreate 256

        // Win32 documentation suggests this does NOT include null-terminator.
        let charsOutput =
            NativeBindings.Win32.GetClassName (hWnd, nameBuffer, 255)

        nameBuffer
        |> Array.take charsOutput
        |> String


    let internal enumChildWindows hWndParent =
        let childHWnds =
            new List<_> ()

        let recordWindowHwnd childHWnd _ =
            do childHWnds.Add childHWnd

            // Return true to continue enumeration.
            true

        let callbackDelegate =
            new NativeBindings.Win32.EnumWindowProc (recordWindowHwnd)

        let _ =
            NativeBindings.Win32.EnumChildWindows (hWndParent, callbackDelegate, 0n)

        seq childHWnds

    


    


        

namespace AnalysisOfChangeEngine.ApiProvider.Excel


open System
open System.Diagnostics
open Microsoft.Office.Interop
open FsToolkit.ErrorHandling

open AnalysisOfChangeEngine.Common


[<AutoOpen>]
module internal Locator =

    let private IID_IExcelWindow =
        // See https://learn.microsoft.com/en-us/dotnet/api/microsoft.office.interop.excel.iwindow?view=excel-pia#:~:text=%5BSystem.Runtime.InteropServices.Guid(%2200020893%2D0001%2D0000%2DC000%2D000000000046%22)%5D
        Guid.Parse("00020893-0000-0000-C000-000000000046")

    let private OBJID_NATIVEOM =
        0xFFFFFFF0u

    let private hWndHasClass className hWnd =
        let className' =
            Win32.getClassName hWnd

        String.Equals (className, className', StringComparison.InvariantCultureIgnoreCase)


    // There are various articles on the web about how to get a COM object for a running
    // application that exposes itself in this way. However, some seemed to be extracting
    // IID_IDispatch from the window, which is not what we want. Instead, I just went straight
    // for the IID_IExcelWindow. Once we have that, we can get everything else that we need.
    let private tryGetExcelApplicationForProcess (``process``: Process) =
        option {
            do! Option.requireTrue 
                    (String.Equals
                        (``process``.ProcessName, "EXCEL", StringComparison.InvariantCultureIgnoreCase))

            let mainWindowHandle =
                ``process``.MainWindowHandle

            do! Option.requireTrue (hWndHasClass "XLMAIN" mainWindowHandle)

            let! excel7ChildWindowHandle =
                Win32.enumChildWindows mainWindowHandle
                |> Seq.tryFind (hWndHasClass "EXCEL7")

            let! excelWindowObj =
                Win32.accessibleObjectFromWindow OBJID_NATIVEOM IID_IExcelWindow excel7ChildWindowHandle
                |> Option.ofResult

            // Let's cast and hope for the best! In all fairness, if we've gotten this far,
            // the logic above was able to find an object with the require COM IID.
            // It would be surprising if it then failed at this point.
            let (excelWindow: Excel.Window) =
                downcast excelWindowObj
            
            // Note to self... Previously I was getting a run-time exception when accessing
            // this member. Repairing the M365 installation seems to have fixed that.
            return excelWindow.Application
        }

    let locateOpenWorkbooks workbookSelector =
        let excelApplications =
            Process.GetProcesses ()
            |> Array.choose tryGetExcelApplicationForProcess

        // We will only include a workbook for a given application provided
        // ONLY ONE workbook within that application satisfies the selector predicate.
        excelApplications
        |> Array.choose (fun app ->
            app.Workbooks
            |> Seq.cast<Excel.Workbook>
            |> Seq.filter (workbookSelector << _.Name)
            |> Seq.tryExactlyOne)

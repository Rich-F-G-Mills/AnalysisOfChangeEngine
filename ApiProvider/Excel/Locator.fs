
namespace AnalysisOfChangeEngine.ApiProvider.Excel


open System
open System.Diagnostics
open Microsoft.Office.Interop
open FsToolkit.ErrorHandling

open AnalysisOfChangeEngine.Common


[<AutoOpen>]
module internal Locator =

    let private IID_IExcelWindow =
        Guid.Parse("00020893-0000-0000-C000-000000000046")

    let private OBJID_NATIVEOM =
        0xFFFFFFF0u

    let private hWndHasClass className hWnd =
        let className' =
            Win32.getClassName hWnd

        String.Equals (className, className', StringComparison.InvariantCultureIgnoreCase)

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

            let (excelWindow: Excel.Window) =
                downcast excelWindowObj

            return excelWindow.Application
        }

    let locateOpenWorkbooks workbookSelector =
        let excelApplications =
            Process.GetProcesses ()
            |> Array.choose tryGetExcelApplicationForProcess

        excelApplications
        |> Array.choose (fun app ->
            app.Workbooks
            |> Seq.cast<Excel.Workbook>
            |> Seq.tryFind (workbookSelector << _.Name))

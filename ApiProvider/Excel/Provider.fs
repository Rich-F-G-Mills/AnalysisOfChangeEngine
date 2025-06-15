
namespace AnalysisOfChangeEngine.ApiProvider.Excel


[<RequireQualifiedAccess>]
module Excel =

    open System
    open System.Diagnostics
    open Microsoft.Office.Interop
    open FsToolkit.ErrorHandling

    open AnalysisOfChangeEngine


    let inline private (=~) (lhs: string) (rhs: string) =
        String.Equals (lhs, rhs, StringComparison.InvariantCultureIgnoreCase)


    let private IID_IExcelWindow =
        Guid.Parse("00020893-0000-0000-C000-000000000046")

    let private OBJID_NATIVEOM =
        0xFFFFFFF0u


    let private hWndHasClass className hWnd =
        let className' =
            Win32.getClassName hWnd

        className' =~ className

    let private tryGetExcelApplicationForProcess (``process``: Process) =
        option {
            do! Option.requireTrue (``process``.ProcessName =~ "EXCEL")

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

    
    let create workbookSelector =
        let excelApplications =
            Process.GetProcesses ()
            |> Array.choose tryGetExcelApplicationForProcess

        // Within each Excel application, find the first workbook (if any) that satisfies the selector.
        let reqdWorkbooks =
            excelApplications
            |> Array.choose (fun app ->
                app.Workbooks
                |> Seq.cast<Excel.Workbook>
                |> Seq.tryFind (workbookSelector << _.Name))

        reqdWorkbooks


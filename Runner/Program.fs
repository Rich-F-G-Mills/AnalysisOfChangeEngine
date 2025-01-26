
namespace AnalysisOfChangeEngine


module Runner =

    open System
    open System.IO
    open Microsoft.VisualBasic.FileIO
    open FsToolkit.ErrorHandling

    open AnalysisOfChangeEngine.Common


    [<EntryPoint>]
    let main _ =
        result {
            let runContext =
                { RunDate = DateOnly.FromDateTime DateTime.Now }

              

            return 0
        }
        |> Result.teeError (printfn "Error: %s")
        |> Result.defaultValue -1
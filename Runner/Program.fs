
namespace AnalysisOfChangeEngine


module Runner =

    open System
    open System.IO
    open System.Text
    open Microsoft.VisualBasic.FileIO
    open FsToolkit.ErrorHandling

    open AnalysisOfChangeEngine.Common
    open AnalysisOfChangeEngine.Implementations
    open AnalysisOfChangeEngine.Implementations.Common
    open AnalysisOfChangeEngine.Controller


    let logger =
        {
            new ILogger with
                member _.LogDebug message =
                    do printfn "DEBUG: %s" message

                member _.LogError message = 
                    do printfn "ERROR: %s" message

                member _.LogInfo message = 
                    do printfn "INFO: %s" message

                member _.LogWarning message = 
                    do printfn "WARNING: %s" message
        }


    [<EntryPoint>]
    let main _ =
        result {
            let walkConfig: OBWholeOfLife.WalkConfiguration =
                {
                    X = 0
                }

            let runContext =
                {
                    OpeningRunDate =
                        DateOnly.FromDateTime DateTime.Now

                    ClosingRunDate =
                        DateOnly.FromDateTime DateTime.Now
                }

            let! walk =
                OBWholeOfLife.Walk.create (logger, runContext, walkConfig)
                
            do printfn "\n\n\n%A" walk.openingRegression.Source

            do printfn "\n\n\n%A" walk.aocOpeningConsistencyCheck.Source

            do printfn "\n\n\n%A" walk.restatedOpeningReturns.Source

            do printfn "\n\n\n%A" (WalkParser.flattenSourceDefinition walk.openingRegression.Source)

            return 0
        }
        |> Result.teeError (printfn "Error: %s")
        |> Result.defaultValue -1
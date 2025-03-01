
namespace AnalysisOfChangeEngine


module Runner =

    open System
    open FsToolkit.ErrorHandling
    open Npgsql
    open Npgsql.FSharp
    open AnalysisOfChangeEngine
    open AnalysisOfChangeEngine.Controller
    open AnalysisOfChangeEngine.Controller.DataStore
    open AnalysisOfChangeEngine.Implementations


    [<RequireQualifiedAccess>]
    type LogLevel =
        | NONE      = 0
        | ERROR     = 1        
        | WARNING   = 2
        | INFO      = 3
        | DEBUG     = 4
        


    let logger (logLevel: LogLevel) =
        {
            new ILogger with
                member _.LogDebug message =
                    if logLevel >= LogLevel.DEBUG then
                        do printfn "DEBUG: %s" message

                member _.LogInfo message = 
                    if logLevel >= LogLevel.INFO then
                        do printfn "INFO: %s" message

                member _.LogError message = 
                    if logLevel >= LogLevel.ERROR then
                        do printfn "ERROR: %s" message

                member _.LogWarning message = 
                    if logLevel >= LogLevel.WARNING then
                        do printfn "WARNING: %s" message
        }


    [<EntryPoint>]
    let main _ =
        result {
            let today =
                DateOnly.FromDateTime DateTime.Now
            
            let closingRunDate =
                new DateOnly (today.Year, today.Month, 1)

            let openingRunDate =
                closingRunDate.AddMonths -1

            let runContext: RunContext =
                {
                    OpeningRunDate =
                        openingRunDate
                    ClosingRunDate =
                        closingRunDate
                }

            let sessionContext: SessionContext =
                { UserName = "RICH" }

            let connStr =
                Sql.host "localhost"
                |> Sql.port 5432
                |> Sql.database "analysis_of_change"
                |> Sql.username "postgres"
                |> Sql.password "internet"
                |> Sql.formatConnectionString

            use connection =
                new NpgsqlConnection (connStr)

            let dataStore =
                new Postgres.DataStore (sessionContext, connection)

            let stepUidResolver =
                let stepHeaders =
                    dataStore.GetAllStepHeaders ()
                    |> Seq.map (fun sh -> sh.Uid, sh)
                    |> Map.ofSeq

                fun uid ->
                    let stepHeader =
                        stepHeaders[uid]

                    stepHeader.Title, stepHeader.Description

            //let newProduct =
            //    dataStore.CreateProduct ("OB Whole-Life", "LVFS CWP OB Whole-Life")

            let walkConfig: OBWholeOfLife.WalkConfiguration =
                {
                    PxDispatcher =
                        new obj ()
                    StepFactory =
                        new StepFactory (stepUidResolver)
                }

            let! walk =
                OBWholeOfLife.Walk.create (logger LogLevel.WARNING, runContext, walkConfig)

            let steps =
                [
                    yield walk.opening :> IStepHeader
                    yield walk.openingRegression :> IStepHeader
                    yield walk.removeExitedRecords :> IStepHeader
                    yield! walk.InteriorSteps
                    yield walk.moveToClosingExistingData :> IStepHeader
                    yield walk.addNewRecords :> IStepHeader
                ]

            do printfn "Steps:"

            for step in steps do
                do printfn "  %s: %s" (step.Title.PadRight 35) step.Description

            do printfn "\n\n\n"

            do printfn "%A" (WalkParser.flattenSourceDefinition walk.openingRegression.Source)

            return 0
        }
        |> Result.teeError (printfn "Error: %s")
        |> Result.defaultValue -1
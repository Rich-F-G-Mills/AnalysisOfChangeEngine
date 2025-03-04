
namespace AnalysisOfChangeEngine


module Runner =

    open System
    open FsToolkit.ErrorHandling
    open Npgsql
    open Npgsql.FSharp
    open AnalysisOfChangeEngine
    open AnalysisOfChangeEngine.Controller
    open AnalysisOfChangeEngine.DataStore
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

            let sessionContext: Postgres.SessionContext =
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

            // The walk creates our api end-points which we can then use below.
            // This means that our logic to define the APIs and the steps is kept
            // together in one place.
            let apiCollection: OBWholeOfLife.ApiCollection =
                {
                    px_OpeningRegression =
                        walk.px_OpeningRegression
                    px_PostOpeningRegression =
                        walk.px_PostOpeningRegression
                }


            do printfn "Steps: (%i found)" (walk.AllSteps |> Seq.length)

            for (idx, step) in walk.AllSteps |> Seq.indexed do
                do printfn "%2i -  %s: %s" idx (step.Title.PadRight 35) step.Description

            do printfn "\n\n\n"

            let parsedWalk =
                WalkParser.parseStepSourcesForWalk apiCollection walk

            do printfn "%A" parsedWalk

            do printfn "\n\n%i" parsedWalk.Length

            return 0
        }
        |> Result.teeError (printfn "Error: %s")
        |> Result.defaultValue -1
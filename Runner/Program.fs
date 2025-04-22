
namespace AnalysisOfChangeEngine


module Runner =

    open System
    open FSharp.Quotations
    open FsToolkit.ErrorHandling
    open Npgsql
    open Npgsql.FSharp
    open AnalysisOfChangeEngine
    open AnalysisOfChangeEngine.Controller.WalkAnalyser
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
                dataStore.CreateUidResolver ()

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

            do printfn "Steps: (%i found)" (walk.AllSteps |> Seq.length)

            for (idx, step) in walk.AllSteps |> Seq.indexed do
                let isDataChange =
                    match step with
                    | :? IDataChangeStep<OBWholeOfLife.PolicyRecord> -> true
                    | _ -> false

                let isSourceChange =
                    match step with
                    | :? ISourceableStep<OBWholeOfLife.PolicyRecord, OBWholeOfLife.StepResults, OBWholeOfLife.ApiCollection> -> true
                    | _ -> false
                    
                do printfn "%2i -  %s %s %s: %s"
                    idx
                    (step.Title.PadRight 35)
                    (if isDataChange then "D" else " ")
                    (if isSourceChange then "S" else " ")
                    step.Description
            
            do printf "\n\nParsing walk... "

            let parsedWalk =
                WalkParser.execute walk.ApiCollection walk            

            do printfn "Done."

            let hdr, parsedStep =
                parsedWalk.OpeningDataStage.WithinStageSteps.Head

            let uas =
                parsedStep.ElementDefinitions["UnsmoothedAssetShare"]

            let polRecord: OBWholeOfLife.PolicyRecord =
                {
                    PolicyNumber        = "TEST"
                    TableCode           = "T01"
                    EntryDate           = new DateOnly (2000, 1, 1)
                    NextPremiumDueDate  = new DateOnly (2024, 1, 1)
                    Status =
                        OBWholeOfLife.PolicyStatus.PremiumPaying
                    Lives =
                        OBWholeOfLife.LivesBasis.SingleLife {
                            EntryAge = 20
                            Gender = OBWholeOfLife.Gender.Male
                        }
                    LimitedPaymentTerm  = 20
                    SumAssured          = 1000.0
                }

            let res =
                uas.WrappedInvoker (polRecord, [|1.0|], [||])

            do printfn "Result: %A" res

            return 0
        }
        |> Result.teeError (printfn "Error: %s")
        |> Result.defaultValue -1
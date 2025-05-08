
namespace AnalysisOfChangeEngine


module Runner =

    open System
    open FsToolkit.ErrorHandling
    open Npgsql
    open Npgsql.FSharp
    open AnalysisOfChangeEngine
    open AnalysisOfChangeEngine.Controller.WalkAnalyser
    open AnalysisOfChangeEngine.DataStore
    open AnalysisOfChangeEngine.DataStore.Postgres
    open AnalysisOfChangeEngine.Walks.Common
    open AnalysisOfChangeEngine.Walks  
    open AnalysisOfChangeEngine.Structures.PolicyRecords
    open AnalysisOfChangeEngine.Structures.StepResults


    [<RequireQualifiedAccess>]
    type LogLevel =
        | NONE      = 0
        | ERROR     = 1        
        | WARNING   = 2
        | INFO      = 3
        | DEBUG     = 4


    (*
    [<NoEquality; NoComparison>]
    type RecordRunStatus =
        {
            
        }
    *)
      

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

            let openingRunUid =
                RunUid (Guid "7526621f-74c3-4edf-8fe8-bebb20c36cd3")

            let closingRunUid =
                RunUid (Guid "7baaafdb-88f0-4d64-b8a6-99040e919307")

            let openingExtractionUid =
                ExtractionUid (Guid "3f1a56c8-9d23-42d7-a5b1-874f01b87e1f")

            let closingExtractionUid =
                ExtractionUid (Guid "d49cb0ab-79e9-4b39-bc0d-47ae8b19e092")

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
                new Postgres.OBWholeOfLife.DataStore (sessionContext, connection)

            let runHeader =
                dataStore.TryGetRunHeader openingRunUid

            let! exitedPolicyRecords, remainingPolicyRecords, newPolicyRecords =
                dataStore.GetPolicyIdDifferences (openingExtractionUid, closingExtractionUid)

            do printfn "Count exiting   : %i" exitedPolicyRecords.Count
            do printfn "Count remaining : %i" remainingPolicyRecords.Count
            do printfn "Count new       : %i" newPolicyRecords.Count
            do printfn "\n\n\n"

            let stepUidResolver =
                dataStore.CreateStepUidResolver ()

            let walkConfig: OBWholeOfLife.WalkConfiguration =
                {
                    PxDispatcher =
                        new obj ()
                    StepFactory =
                        new StepFactory (stepUidResolver)
                    IgnoreOpeningMismatches =
                        false
                }

            let! walk =
                OBWholeOfLife.Walk.create (logger LogLevel.WARNING, runContext, walkConfig)

            //let _ =
            //    dataStore.CreateRun ("Monthly MI", None, None, openingRunDate, openingExtractionUid, walk)

            //let _ =
            //    dataStore.CreateRun ("Monthly MI", None, Some openingRunUid, closingRunDate, closingExtractionUid, walk)

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
                    TableCode           = "A"
                    Taxable             = true
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
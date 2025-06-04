
namespace AnalysisOfChangeEngine


module Runner =

    open System
    open FsToolkit.ErrorHandling
    open Npgsql
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
                RunUid (Guid "2ba4d2d5-5c79-4e35-b435-fb8a54713fd1")

            let closingRunUid =
                RunUid (Guid "2c31b642-b0d5-450c-b44b-54ebf1790a85")

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

            let connStrBdr =
                new NpgsqlConnectionStringBuilder(
                    Host        = "localhost",
                    Port        = 5432,
                    Database    =  "analysis_of_change",
                    Username    =  "postgres",
                    Password    = "internet"
                )

            let dataSourceBdr =
                new NpgsqlDataSourceBuilder (connStrBdr.ConnectionString)

            let dataSource =
                dataSourceBdr.Build ()

            let dataStore =
                new Postgres.OBWholeOfLife.DataStore (sessionContext, dataSource)

            //let runHeader =
            //    dataStore.TryGetRunHeader openingRunUid

            let! exitedPolicyRecords, remainingPolicyRecords, newPolicyRecords =
                dataStore.GetPolicyIdDifferences (openingExtractionUid, closingExtractionUid)

            do printfn "Count exiting   : %i" exitedPolicyRecords.Count
            do printfn "Count remaining : %i" remainingPolicyRecords.Count
            do printfn "Count new       : %i" newPolicyRecords.Count
            do printfn "\n"

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

            //let openingRun =
            //    dataStore.CreateRun ("Monthly MI", None, None, openingRunDate, openingExtractionUid, walk)

            //let closingRun =
            //    dataStore.CreateRun ("Monthly MI", None, Some openingRun.Uid, closingRunDate, closingExtractionUid, walk)

            let! openingRun =
                dataStore.TryGetRunHeader openingRunUid
                |> Result.requireSome "Unable to locate opening run header."

            let! closingRun =
                dataStore.TryGetRunHeader closingRunUid
                |> Result.requireSome "Unable to locate closing run header."

            do printfn "Opening run UID: %O" openingRun.Uid.Value
            do printfn "Closing run UID: %O\n\n" closingRun.Uid.Value

            do printfn "Steps: (%i found)" (walk.AllSteps |> Seq.length)

            for (idx, step) in Seq.indexed walk.AllSteps do
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

            let rawPolRecord: OBWholeOfLife.RawPolicyRecord =
                {
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
                    SumAssured          = 1000.0f
                }

            let! polRecord =
                OBWholeOfLife.PolicyRecord.validate rawPolRecord

            let res =
                uas.WrappedInvoker (polRecord, [|1.0f|], [||])

            do printfn "Result: %A" res

            return 0
        }
        |> Result.teeError (printfn "Error: %s")
        |> Result.defaultValue -1
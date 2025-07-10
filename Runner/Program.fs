
namespace AnalysisOfChangeEngine


module Runner =

    open System
    open FsToolkit.ErrorHandling
    open Npgsql
    open AnalysisOfChangeEngine
    open AnalysisOfChangeEngine.Controller
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

            let priorRunUid =
                RunUid (Guid "1dd045cb-068c-4a55-842f-fd37722de30b")

            let currentRunUid =
                RunUid (Guid "3d8ddea7-992b-4c13-ab70-85d1697b0304")

            let priorExtractionUid =
                ExtractionUid (Guid "3f1a56c8-9d23-42d7-a5b1-874f01b87e1f")

            let currentExtractionUid =
                ExtractionUid (Guid "d49cb0ab-79e9-4b39-bc0d-47ae8b19e092")

            let openingRunDate =
                closingRunDate.AddMonths -1

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

            let stepUidResolver =
                dataStore.CreateStepUidResolver ()

            let walkConfig: OBWholeOfLife.WalkConfiguration =
                {
                    StepFactory             = new StepFactory (stepUidResolver)
                    IgnoreOpeningMismatches = false
                    OpeningRunDate          = Some openingRunDate
                    ClosingRunDate          = closingRunDate
                }

            let! walk =
                OBWholeOfLife.Walk.create (logger LogLevel.WARNING, walkConfig)

            //let priorRun =
            //    dataStore.CreateRun ("Monthly MI", None, None, openingRunDate, openingExtractionUid, walk)

            //let currentRun =
            //    dataStore.CreateRun ("Monthly MI", None, Some priorRun.Uid, closingRunDate, closingExtractionUid, walk)

            let! priorRun =
                dataStore.TryGetRunHeader priorRunUid
                |> Result.requireSome "Unable to locate prior run header."

            let! currentRun =
                dataStore.TryGetRunHeader currentRunUid
                |> Result.requireSome "Unable to locate current run header."

            let! outstandingRecords =
                dataStore.TryGetOutstandingRecords currentRun.Uid

            do printfn "\n\nOutstanding records: %i\n\n" outstandingRecords.Length

            do printfn "Opening run UID: %O" priorRun.Uid.Value
            do printfn "Closing run UID: %O\n\n" currentRun.Uid.Value

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


            let somePolicyIds =
                outstandingRecords
                |> Seq.choose (function
                    | Choice1Of3 (ExitedPolicyId policyId) ->
                        Some policyId
                    | _ ->
                        None) 
                |> Seq.take 5
                |> Seq.toArray

            let someExitedPolicyRecords =
                dataStore.GetPolicyRecordsAsync priorExtractionUid somePolicyIds
                |> _.Result
                |> Map.map (fun _ -> Result.map ExitedPolicy)
                |> Map.map (fun _ -> Result.defaultWith (fun _ -> failwith "Failed"))

            do printfn "\n\n%A\n\n" someExitedPolicyRecords

            
            let exitedEvaluator =
                Evaluator.exitedPolicyEvaluator walk walk.ApiCollection
            
            let results =
                someExitedPolicyRecords
                |> Map.map (fun _ -> exitedEvaluator)
                |> Map.map (fun _ -> _.Result)

            do printfn "\n\n%A\n\n" results

            return 0
        }
        |> Result.teeError (printfn "Error: %s")
        |> Result.defaultValue -1

namespace AnalysisOfChangeEngine


module Runner =

    open System
    open System.IO
    open System.Reactive.Linq
    open System.Reactive.Concurrency
    open System.Text
    open System.Text.Json    
    open System.Threading.Tasks
    open FsToolkit.ErrorHandling
    open Npgsql
    open AnalysisOfChangeEngine
    open AnalysisOfChangeEngine.Controller
    open AnalysisOfChangeEngine.Controller.CalculationLoop
    open AnalysisOfChangeEngine.Controller.Telemetry
    open AnalysisOfChangeEngine.DataStore
    open AnalysisOfChangeEngine.DataStore.Postgres
    open AnalysisOfChangeEngine.Walks
    open AnalysisOfChangeEngine.Walks.Common
    open AnalysisOfChangeEngine.Structures.PolicyRecords
    open AnalysisOfChangeEngine.Structures.StepResults


    [<RequireQualifiedAccess>]
    type LogLevel =
        | NONE      = 0
        | ERROR     = 1        
        | WARNING   = 2
        | INFO      = 3
        | DEBUG     = 4
     

    let createLogger (logLevel: LogLevel) =
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
            let logger =
                createLogger LogLevel.DEBUG
            
            let closingRunDate =
                new DateOnly (2025, 2, 1)

            //let priorRunUid =
            //    RunUid (Guid "1368816c-979e-4fde-8e0d-73e8d4f9b8de")

            let currentRunUid =
                RunUid (Guid "40b70255-1c18-488f-821c-492a2835c810")

            //let priorExtractionUid =
            //    ExtractionUid (Guid "3f1a56c8-9d23-42d7-a5b1-874f01b87e1f")

            //let currentExtractionUid =
            //    ExtractionUid (Guid "d49cb0ab-79e9-4b39-bc0d-47ae8b19e092")

            let sessionUid =
                SessionUid (Guid.NewGuid ())

            let openingRunDate =
                closingRunDate.AddMonths -1

            let sessionContext: SessionContext =
                { UserName = "RICH" }

            let connStrBdr =
                new NpgsqlConnectionStringBuilder(
                    Host        = "localhost",
                    Port        = 5432,
                    Database    = "analysis_of_change",
                    Username    = "postgres",
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

            let! walk =
                OBWholeOfLife.Walk.create logger {
                    StepFactory             = new StepFactory (stepUidResolver)
                    IgnoreOpeningMismatches = true
                    OpeningRunDate          = Some openingRunDate
                    ClosingRunDate          = closingRunDate
                }

            //let priorRunUid =
            //    dataStore.CreateRun ("Monthly MI", None, None, openingRunDate, priorExtractionUid, walk)

            //let currentRunUid =
            //    dataStore.CreateRun ("Monthly MI", None, Some priorRunUid, closingRunDate, currentExtractionUid, walk)

            let! currentRunHeader =
                dataStore.TryGetRunHeader currentRunUid
                |> Result.requireSome "Unable to locate current run header."

            let! priorRunHeader =
                currentRunHeader.PriorRunUid
                |> Option.bind dataStore.TryGetRunHeader 
                |> Result.requireSome "Unable to locate prior run header."

            do printf "\n\nRetrieving outstanding records... "

            let! outstandingRecords =
                dataStore.TryGetOutstandingRecords currentRunHeader.RunUid {
                    ReRunFailedCases = true
                }
            
            do printfn "%i\n\n" outstandingRecords.Length

            do printfn "Opening run UID: %O" priorRunHeader.RunUid.Value
            do printfn "Closing run UID: %O\n\n" currentRunHeader.RunUid.Value
            do printfn "Session UID    : %O\n\n" sessionUid.Value

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

            do printfn "\n\n"

            let calculationLoop =
                createCalculationLoop {
                    OpeningPolicyReader =
                        dataStore.CreatePolicyGetter priorRunHeader.PolicyDataExtractionUid
                    ClosingPolicyReader =
                        dataStore.CreatePolicyGetter currentRunHeader.PolicyDataExtractionUid
                    PriorClosingStepResultReader =
                        dataStore.CreateStepResultsGetter priorRunHeader.RunUid priorRunHeader.ClosingStepUid
                    WalkEvaluator =
                        Evaluator.create logger walk walk.ApiCollection
                    OutputWriter =
                        dataStore.CreateOutputWriter (currentRunUid, sessionUid)
                }

            use scheduler =
                new EventLoopScheduler ()

            use fileStream =
                new FileStream ($"""C:\Users\Millch\Documents\AnalysisOfChangeEngine\Results Viewer\TELEMETRY\{sessionUid.Value}.json""", FileMode.Create)

            use jsonWriter =
                new Utf8JsonWriter (fileStream)

            do jsonWriter.WriteStartArray ()

            use _ =
                { new IDisposable with
                    member _.Dispose () =
                        do jsonWriter.WriteEndArray ()
                }

            let onTelemetryComplete =
                new TaskCompletionSource ()

            let jsonSerializerOptions =
                new JsonSerializerOptions(
                    DefaultIgnoreCondition =
                        Serialization.JsonIgnoreCondition.WhenWritingNull
                )

            let inline serialiseData (eventType, eventData) =
                let wrappedData =
                    {|
                        run_uid     = currentRunUid.Value
                        session_uid = sessionUid.Value
                        event_type  = eventType
                        event_data  = eventData
                    |}

                do JsonSerializer.Serialize
                    (jsonWriter, wrappedData, jsonSerializerOptions) 

            let writeEvent = function
                | TelemetryEvent.ApiRequest data ->
                    do serialiseData (JsonFormatter.format data)
                | TelemetryEvent.RecordSubmitted data ->
                    do serialiseData (JsonFormatter.format data)
                | TelemetryEvent.PolicyRead data ->
                    do serialiseData (JsonFormatter.format data)
                | TelemetryEvent.EvaluationCompleted data ->
                    do printf "%s" (if data.HadFailures then "!" else ".")
                    do serialiseData (JsonFormatter.format data)
                | TelemetryEvent.PolicyWrite data ->
                    do serialiseData (JsonFormatter.format data)
                | TelemetryEvent.DataStoreRead data ->
                    do printf "R"
                    do serialiseData (JsonFormatter.format data)
                | TelemetryEvent.DataStoreWrite data ->
                    do printf "W"
                    do serialiseData (JsonFormatter.format data)
                    
            use _ =
                calculationLoop.Telemetry
                    .ObserveOn(scheduler)
                    .Subscribe(
                        writeEvent,
                        (fun (exn: exn) ->
                            do serialiseData ("loop_failure", {| reason = exn.Message |})
                            do onTelemetryComplete.SetResult ()),
                        (fun () ->
                            do serialiseData ("session_end", {| |})
                            do onTelemetryComplete.SetResult ()))

            let someOutstandingRecords =
                outstandingRecords
                //|> List.take 100

            let runner =
                backgroundTask {
                    for record in someOutstandingRecords do
                        let! _ =
                            match record with
                            | Choice1Of3 (ExitedPolicyId _ as pid) ->
                                calculationLoop.PostAsync pid
                            | Choice2Of3 (RemainingPolicyId _ as pid) ->
                                calculationLoop.PostAsync pid
                            | Choice3Of3 (NewPolicyId _ as pid) ->
                                calculationLoop.PostAsync pid

                        ()

                    // Notify the machinery that we have no more records to process.
                    do calculationLoop.Complete ()

                    // Wait for the calculation loop to complete.
                    do! calculationLoop.Completion

                    // Wait for our telemetry subscriber to complete.
                    do! onTelemetryComplete.Task
                }

            do runner.Wait ()

            return 0
        }
        |> Result.teeError (printfn "Error: %s")
        |> Result.defaultValue -1

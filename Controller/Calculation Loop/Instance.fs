
namespace AnalysisOfChangeEngine.Controller.CalculationLoop   


[<AutoOpen>]
module CalculationLoop =

    open System
    open System.Threading
    open System.Threading.Tasks
    open System.Threading.Tasks.Dataflow
    open System.Reactive.Linq
    open System.Reactive.Subjects
    open AnalysisOfChangeEngine
    open AnalysisOfChangeEngine.Controller


    type internal CalculationLoop<'TPolicyRecord, 'TStepResults> internal
        (openingPolicyGetter: IPolicyGetter<'TPolicyRecord> option,
         priorClosingStepResultGetter: IStepResultsGetter<'TStepResults> option,
         closingPolicyGetter: IPolicyGetter<'TPolicyRecord>,
         walkEvaluator: IPolicyWalkEvaluator<'TPolicyRecord, 'TStepResults>,
         outputWriter: IProcessedOutputWriter<'TPolicyRecord, 'TStepResults>) =

            let mutable currentReadIdx = -1
            let mutable currentWriteIdx = -1

            let telemetrySubject =
                new Subject<TelemetryEvent> ()       

            let notifyApiRequestSubmitted policyId dataSource (RequestorName requestorName) =
                // Potentially, there may be a delay until we actually start to do something!
                let requestSubmitted =
                    DateTime.Now

                fun endpointId ->
                    let processingStart =
                        DateTime.Now

                    {
                        new IDisposable with
                            member _.Dispose () =
                                let apiRequestTelemetry =
                                    TelemetryEvent.ApiRequest {
                                        PolicyId            = policyId
                                        RequestorName       = requestorName
                                        DataSource          = dataSource
                                        EndpointId          = endpointId |> Option.map (function | EndpointId epid -> epid)
                                        RequestSubmitted    = requestSubmitted
                                        ProcessingStart     = processingStart
                                        ProcessingEnd       = DateTime.Now
                                    }

                                do telemetrySubject.OnNext apiRequestTelemetry       
                    }

            let policyReader =
                let inner =
                    match openingPolicyGetter, priorClosingStepResultGetter with
                    | Some openingPolicyGetter', Some priorClosingStepResultGetter' ->
                        openingAndClosingReader (openingPolicyGetter', priorClosingStepResultGetter', closingPolicyGetter)
                    | None, None ->
                        closingOnlyReader closingPolicyGetter
                    | _ ->
                        failwith "Unexpected loop parameters."

                fun (pendingPolicyIds: CohortedPolicyId array) ->
                    backgroundTask {
                        let readStart =
                            DateTime.Now

                        let! policyRecords =
                            inner pendingPolicyIds

                        let readEnd =
                            DateTime.Now

                        // The current assumption is that this function will never be called concurrently.
                        // However, this just future-proofs it!
                        let newReadIdx =
                            Interlocked.Increment &currentReadIdx

                        let readTelemetryEvent =
                            {
                                Idx         = newReadIdx
                                ReadStart   = readStart
                                ReadEnd     = readEnd
                            }

                        do telemetrySubject.OnNext (TelemetryEvent.DataStoreRead readTelemetryEvent)

                        let requestsPendingEvaluation =
                            (pendingPolicyIds, policyRecords)
                            ||> Seq.map2 (fun pendingPolicyId policyRecord ->
                                    {
                                        PolicyId            = pendingPolicyId
                                        PolicyRecord        = policyRecord
                                        DataReadIdx         = newReadIdx
                                    })

                        return requestsPendingEvaluation
                    }

            let policyEvaluator = function
                | { PolicyRecord = Ok policyRecord } as request ->
                    backgroundTask {
                        let evaluationStart =
                            DateTime.Now

                        let notifyApiRequestSubmitted' =
                            notifyApiRequestSubmitted request.PolicyId.Underlying

                        let! evaluationOutcome =
                            match policyRecord with
                            | CohortedPolicyRecord.Exited (policyRecord, priorClosingResults) ->
                                walkEvaluator.Execute (ExitedPolicy policyRecord, priorClosingResults, notifyApiRequestSubmitted')
                            | CohortedPolicyRecord.Remaining (openingPolicyRecord, closingPolicyRecord, priorClosingResults) ->
                                walkEvaluator.Execute (RemainingPolicy (openingPolicyRecord, closingPolicyRecord), priorClosingResults, notifyApiRequestSubmitted')
                            | CohortedPolicyRecord.New policyRecord ->
                                walkEvaluator.Execute (NewPolicy policyRecord, notifyApiRequestSubmitted')

                        return (OutputRequest.CompletedEvaluation {
                            PolicyId        = request.PolicyId
                            EvaluationStart = evaluationStart
                            EvaluationEnd   = DateTime.Now
                            WalkOutcome     = evaluationOutcome
                            DataReadIdx     = request.DataReadIdx
                        })
                    }

                | { PolicyRecord = Error readFailures } as request ->
                    backgroundTask {
                        return (OutputRequest.FailedPolicyRead {
                            PolicyId        = request.PolicyId
                            FailureReasons  = readFailures
                            DataReadIdx     = request.DataReadIdx
                        })
                    }

            let outputWriter writeRequests : Task =
                backgroundTask {
                    let newWriteIdx =
                        Interlocked.Increment &currentWriteIdx

                    let processedOutputs =
                        writeRequests
                        |> Array.map (function
                            | OutputRequest.CompletedEvaluation request ->
                                let telemetryEvent =
                                    TelemetryEvent.ProcessingCompleted {
                                        PolicyId            = request.PolicyId.Underlying
                                        EvaluationStart     = request.EvaluationStart
                                        EvaluationEnd       = request.EvaluationEnd
                                        DataStoreReadIdx    = request.DataReadIdx
                                        DataStoreWriteIdx   = newWriteIdx
                                    }

                                do telemetrySubject.OnNext telemetryEvent

                                match request.WalkOutcome with
                                | Ok evaluatedWalk ->
                                    {
                                        PolicyId    = request.PolicyId.Underlying
                                        WalkOutcome = Ok evaluatedWalk
                                    }

                                | Error evaluationFailure ->
                                    {
                                        PolicyId    = request.PolicyId.Underlying
                                        WalkOutcome = Error (ProcessedPolicyFailure.EvaluationFailures evaluationFailure)
                                    }

                            | OutputRequest.FailedPolicyRead request ->
                                let telemetryEvent =
                                    TelemetryEvent.FailedPolicyRead {
                                        PolicyId            = request.PolicyId.Underlying
                                        DataStoreReadIdx    = request.DataReadIdx
                                        DataStoreWriteIdx   = newWriteIdx
                                    }

                                do telemetrySubject.OnNext telemetryEvent

                                {
                                    PolicyId    = request.PolicyId.Underlying
                                    WalkOutcome = Error (ProcessedPolicyFailure.ReadFailures request.FailureReasons)
                                }
                        )

                    let writeStart =
                        DateTime.Now

                    do! outputWriter.WriteProcessedOutputAsync processedOutputs

                    let writeEnd =
                        DateTime.Now

                    let writeTelemetryEvent =
                        TelemetryEvent.DataStoreWrite {
                            Idx                 = newWriteIdx
                            WriteStart          = writeStart
                            WriteEnd            = writeEnd
                        }

                    do telemetrySubject.OnNext writeTelemetryEvent
                }                
                

            let policyIdBatcher =
                new BatchBlock<_> (
                    25,
                    new GroupingDataflowBlockOptions (
                        BoundedCapacity = 100
                    )
                )

            let policyReaderBlock =
                new TransformManyBlock<_, _> (
                    policyReader,
                    new ExecutionDataflowBlockOptions (
                        BoundedCapacity = 1000,
                        // Ensure that we only ever process a single batch of reads at a time.
                        MaxDegreeOfParallelism = 1
                    )
                )

            let policyEvaluationBlock =
                new TransformBlock<_, OutputRequest<_, _>> (
                    policyEvaluator,
                    new ExecutionDataflowBlockOptions (
                        BoundedCapacity = 100,
                        MaxDegreeOfParallelism = 5
                    )
                )

            let outputBatcher =
                new BatchBlock<_> (
                    25,
                    new GroupingDataflowBlockOptions (
                        BoundedCapacity = 100
                    )
                )

            let writerBlock =
                new ActionBlock<_> (
                    outputWriter,
                    new ExecutionDataflowBlockOptions (
                        BoundedCapacity = 100,
                        MaxDegreeOfParallelism = 1
                    )
                )

            let linkOptions =
                new DataflowLinkOptions (
                    PropagateCompletion = true
                )

            // TODO - Should we care about these?
            let _ =
                policyIdBatcher.LinkTo (policyReaderBlock, linkOptions)

            let _ =
                policyReaderBlock.LinkTo (policyEvaluationBlock, linkOptions)

            let _ =
                policyEvaluationBlock.LinkTo (outputBatcher, linkOptions)

            let _ =
                outputBatcher.LinkTo (writerBlock, linkOptions)


            member internal _.PostAsync policyId =
                policyIdBatcher.SendAsync policyId

            member val internal Telemetry =
                telemetrySubject.AsObservable ()

            member _.Complete () =
                do policyIdBatcher.Complete ()

            member val Completion =
                writerBlock.Completion
                    // Only consider ourselves completed once we've already notified
                    // as such with the telemetry observable.
                    .ContinueWith (fun _ -> do telemetrySubject.OnCompleted ())


    type INewOnlyCalculationLoop<'TPolicyRecord> =
        abstract member PostAsync   : NewPolicyId          -> Task<bool>
        abstract member Telemetry   : IObservable<TelemetryEvent>
        abstract member Complete    : Unit -> Unit
        abstract member Completion  : Task

    type ICalculationLoop<'TPolicyRecord> =
        inherit INewOnlyCalculationLoop<'TPolicyRecord>

        abstract member PostAsync   : ExitedPolicyId       -> Task<bool>
        abstract member PostAsync   : RemainingPolicyId    -> Task<bool>

    
    let createClosingOnlyCalculationLoop<'TPolicyRecord, 'TStepResults>
        (closingPolicyGetter, walkEvaluator, outputWriter) =
            let calcLoop =
                new CalculationLoop<'TPolicyRecord, 'TStepResults>
                    (None, None, closingPolicyGetter, walkEvaluator, outputWriter)

            {
                new INewOnlyCalculationLoop<'TPolicyRecord> with
                    member _.PostAsync (NewPolicyId policyId) =
                        calcLoop.PostAsync (CohortedPolicyId.New policyId)

                    member _.Telemetry
                        with get () = calcLoop.Telemetry

                    member _.Complete () =
                        do calcLoop.Complete ()

                    member _.Completion
                        with get() = calcLoop.Completion
            }

    let createCalculationLoop<'TPolicyRecord, 'TStepResults>
        (openingPolicyReader, priorClosingStepResultGetter, closingPolicyGetter, walkEvaluator, outputWriter) =
            let calcLoop =
                new CalculationLoop<'TPolicyRecord, 'TStepResults>
                    (Some openingPolicyReader, Some priorClosingStepResultGetter, closingPolicyGetter, walkEvaluator, outputWriter)

            {
                new ICalculationLoop<'TPolicyRecord> with
                    member _.PostAsync (ExitedPolicyId policyId) =
                        calcLoop.PostAsync (CohortedPolicyId.Exited policyId)

                    member _.PostAsync (RemainingPolicyId policyId) =
                        calcLoop.PostAsync (CohortedPolicyId.Remaining policyId)

                    member _.PostAsync (NewPolicyId policyId) =
                        calcLoop.PostAsync (CohortedPolicyId.New policyId)

                    member _.Telemetry
                        with get () = calcLoop.Telemetry

                    member _.Complete () =
                        do calcLoop.Complete ()

                    member _.Completion
                        with get() = calcLoop.Completion
            }


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
    open AnalysisOfChangeEngine.Controller.Telemetry


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
                                        EndpointId          =
                                            endpointId |> Option.map (function | EndpointId epid -> epid)
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

                        // The current assumption is that this function will never be called concurrently.
                        // However, this just future-proofs it!
                        let newReadIdx =
                            Interlocked.Increment &currentReadIdx

                        do  telemetrySubject.OnNext (TelemetryEvent.DataStoreRead {
                                Idx         = newReadIdx
                                ReadStart   = readStart
                                ReadEnd     = DateTime.Now
                            })

                        let requestsPendingEvaluation =
                            (pendingPolicyIds, policyRecords)
                            ||> Seq.map2 (fun pendingPolicyId policyRecord ->
                                    {
                                        PolicyId        = pendingPolicyId
                                        PolicyRecord    = policyRecord
                                    })

                        do  (pendingPolicyIds, policyRecords)
                            ||> Seq.iter2 (fun pendingPolicyId policyRecord ->
                                    do telemetrySubject.OnNext (TelemetryEvent.PolicyRead {
                                        PolicyId            = pendingPolicyId.Underlying
                                        DataStoreReadIdx    = newReadIdx
                                        HadFailures         = policyRecord.IsError
                                    }))

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

                        do telemetrySubject.OnNext (TelemetryEvent.EvaluationCompleted {
                            PolicyId            = request.PolicyId.Underlying
                            EvaluationStart     = evaluationStart
                            EvaluationEnd       = DateTime.Now
                            HadFailures         = evaluationOutcome.IsError
                        })

                        return (OutputRequest.CompletedEvaluation {
                            PolicyId            = request.PolicyId
                            WalkOutcome         = evaluationOutcome
                        })
                    }

                | { PolicyRecord = Error readFailures } as request ->
                    backgroundTask {
                        return (OutputRequest.FailedPolicyRead {
                            PolicyId            = request.PolicyId
                            FailureReasons      = readFailures
                        })
                    }

            let outputWriter writeRequests : Task =
                backgroundTask {                    
                    let writeStart =
                        DateTime.Now

                    let newWriteIdx =
                        Interlocked.Increment &currentWriteIdx

                    let processedOutputs =
                        writeRequests
                        |> Array.map (function
                            | OutputRequest.CompletedEvaluation request ->
                                match request.WalkOutcome with
                                | Ok evaluatedWalk ->
                                    {
                                        PolicyId    = request.PolicyId.Underlying
                                        WalkOutcome = Ok evaluatedWalk
                                    }

                                | Error evaluationFailures ->
                                    {
                                        PolicyId    = request.PolicyId.Underlying
                                        WalkOutcome =
                                            Error (ProcessedPolicyFailure.EvaluationFailures evaluationFailures)
                                    }

                            | OutputRequest.FailedPolicyRead request ->
                                {
                                    PolicyId    = request.PolicyId.Underlying
                                    WalkOutcome =
                                        Error (ProcessedPolicyFailure.ReadFailures request.FailureReasons)
                                }
                        )

                    let! writerOutcomes =
                        outputWriter.WriteProcessedOutputAsync processedOutputs

                    do assert (processedOutputs.Length = writerOutcomes.Length)

                    do  (processedOutputs, writerOutcomes)
                        ||> Seq.iter2 (fun processOutput writerOutcome ->
                                do telemetrySubject.OnNext (TelemetryEvent.PolicyWrite {
                                    PolicyId            = processOutput.PolicyId
                                    DataStoreWriteIdx   = newWriteIdx
                                    HadFailures         = writerOutcome.FailuresGenerated
                                }))

                    do telemetrySubject.OnNext (TelemetryEvent.DataStoreWrite {
                        Idx         = newWriteIdx
                        WriteStart  = writeStart
                        WriteEnd    = DateTime.Now
                    })
                }                
                

            let policyIdBatcher =
                new BatchBlock<_> (
                    50,
                    new GroupingDataflowBlockOptions (
                        BoundedCapacity = 75
                    )
                )

            let policyReaderBlock =
                new TransformManyBlock<_, _> (
                    policyReader,
                    new ExecutionDataflowBlockOptions (
                        BoundedCapacity = 2,
                        // Ensure that we only ever process a single batch of reads at a time.
                        MaxDegreeOfParallelism = 1
                    )
                )

            let policyEvaluationBlock =
                new TransformBlock<_, OutputRequest<_, _>> (
                    policyEvaluator,
                    new ExecutionDataflowBlockOptions (
                        BoundedCapacity = 25,
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
                        BoundedCapacity = 2,
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
                    .ContinueWith (fun completedTask ->
                        // Where we are propogating completions, this will capture faults
                        // at any point along the chain.
                        if completedTask.Status = TaskStatus.Faulted then
                            do telemetrySubject.OnError (completedTask.Exception)
                        else
                            // Note that this will be called regardless of why the writer block completed.
                            // Completion could have occurred due to cancellation, an error, or normal completion.
                            // Further more, the on completed notification below may be non-blocking depending
                            // on what scheduler is being used. Suffice to say, if there is on-complete logic
                            // that MUST be run, the user will have to handle that as part of their own logic.
                            do telemetrySubject.OnCompleted ())


    type INewOnlyCalculationLoop<'TPolicyRecord> =
        abstract member PostAsync   : NewPolicyId          -> Task<bool>
        abstract member Telemetry   : IObservable<TelemetryEvent>
        abstract member Complete    : Unit -> Unit
        abstract member Completion  : Task

    type ICalculationLoop<'TPolicyRecord> =
        inherit INewOnlyCalculationLoop<'TPolicyRecord>

        abstract member PostAsync   : ExitedPolicyId       -> Task<bool>
        abstract member PostAsync   : RemainingPolicyId    -> Task<bool>


    [<NoEquality; NoComparison>]
    type ClosingOnlyCalculationLoopConfiguration<'TPolicyRecord, 'TStepResults> =
        {
            PriorClosingPolicyReader    : IPolicyGetter<'TPolicyRecord>
            WalkEvaluator               : IPolicyWalkEvaluator<'TPolicyRecord, 'TStepResults>
            OutputWriter                : IProcessedOutputWriter<'TPolicyRecord, 'TStepResults>
        }

    [<NoEquality; NoComparison>]
    type CalculationLoopConfiguration<'TPolicyRecord, 'TStepResults> =
        {
            OpeningPolicyReader             : IPolicyGetter<'TPolicyRecord>
            PriorClosingStepResultReader    : IStepResultsGetter<'TStepResults>
            ClosingPolicyReader             : IPolicyGetter<'TPolicyRecord>
            WalkEvaluator                   : IPolicyWalkEvaluator<'TPolicyRecord, 'TStepResults>
            OutputWriter                    : IProcessedOutputWriter<'TPolicyRecord, 'TStepResults>
        }

    
    /// Create a calculation loop that can only process new policies.
    let createClosingOnlyCalculationLoop
        (config: ClosingOnlyCalculationLoopConfiguration<'TPolicyRecord, 'TStepResults>) =
            let calcLoop =
                new CalculationLoop<'TPolicyRecord, 'TStepResults>
                    (None, None, config.PriorClosingPolicyReader, config.WalkEvaluator, config.OutputWriter)

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

    /// Create a calculation loop that can process all cohorts of policy.
    let createCalculationLoop
        (config: CalculationLoopConfiguration<'TPolicyRecord, 'TStepResults>) =
            let calcLoop =
                new CalculationLoop<'TPolicyRecord, 'TStepResults>
                    (Some config.OpeningPolicyReader, Some config.PriorClosingStepResultReader,
                     config.ClosingPolicyReader, config.WalkEvaluator, config.OutputWriter)

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

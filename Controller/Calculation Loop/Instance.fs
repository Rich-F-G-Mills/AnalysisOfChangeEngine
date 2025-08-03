
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


    let private notifyApiRequestSubmitted receiver policyId dataSource (RequestorName requestorName) =
        // Potentially, there may be a delay until we actually start to do something!
        let requestSubmitted =
            DateTime.Now

        fun endpointId ->
            let processingStart =
                DateTime.Now

            {
                new IDisposable with
                    member _.Dispose () =
                        receiver {
                            PolicyId            = policyId
                            RequestorName       = requestorName
                            DataSource          = dataSource
                            EndpointId          = endpointId |> Option.map (function | EndpointId epid -> epid)
                            RequestSubmitted    = requestSubmitted
                            ProcessingStart     = processingStart
                            ProcessingEnd       = DateTime.Now                     
                        }       
            }


    type internal CalculationLoop<'TPolicyRecord, 'TStepResults>
        (openingPolicyGetter: IPolicyGetter<'TPolicyRecord> option,
         priorClosingStepResultGetter: IStepResultsGetter<'TStepResults> option,
         closingPolicyGetter: IPolicyGetter<'TPolicyRecord>,
         walkEvaluator: IPolicyWalkEvaluator<'TPolicyRecord, 'TStepResults>) =

            let mutable currentReadIdx = -1
            let mutable currentWriteIdx = -1

            let telemetrySubject =
                new Subject<TelemetryEvent> ()       

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

                        let! evaluationOutcome =
                            match policyRecord with
                            | CohortedPolicyRecord.Exited (policyRecord, priorClosingResults) ->
                                walkEvaluator.Execute (ExitedPolicy policyRecord, priorClosingResults)
                            | CohortedPolicyRecord.Remaining (openingPolicyRecord, closingPolicyRecord, priorClosingResults) ->
                                walkEvaluator.Execute (RemainingPolicy (openingPolicyRecord, closingPolicyRecord), priorClosingResults)
                            | CohortedPolicyRecord.New policyRecord ->
                                walkEvaluator.Execute (NewPolicy policyRecord)

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
                new TransformBlock<_, _> (
                    null,
                    new DataflowBlockOptions (
                        BoundedCapacity = 100,
                        MaxDegreeOfParallelism = 5
                    )
                )

            let linkOptions =
                new DataflowLinkOptions (
                    PropagateCompletion = true
                )

            let readerLink =
                policyIdBatcher.LinkTo (policyReaderBlock, linkOptions)


            member internal _.PostAsync (policyId) =
                policyIdBatcher.SendAsync {
                    PolicyId = policyId
                    RequestSubmitted = DateTime.Now
                }

            member val internal Telemetry =
                telemetrySubject.AsObservable ()


    type INewOnlyCalculationLoop<'TPolicyRecord> =
        inherit IObservable<TelemetryEvent>
        inherit IDisposable

        abstract member PostAsync: NewPolicyId          -> Task<bool>

    type ICalculationLoop<'TPolicyRecord> =
        inherit INewOnlyCalculationLoop<'TPolicyRecord>
        inherit IObservable<TelemetryEvent>
        inherit IDisposable

        abstract member PostAsync: ExitedPolicyId       -> Task<bool>
        abstract member PostAsync: RemainingPolicyId    -> Task<bool>        


    [<RequireQualifiedAccess>]
    module CalculationLoop =
    
        let createClosingOnly<'TPolicyRecord, 'TStepResults> (closingPolicyReader) =
            let calcLoop =
                new CalculationLoop<'TPolicyRecord, 'TStepResults>
                    (None, None, closingPolicyReader)

            {
                new INewOnlyCalculationLoop<'TPolicyRecord> with
                    member this.PostAsync (NewPolicyId policyId) =
                        calcLoop.PostAsync (CohortedPolicyId.New policyId)

                interface IObservable<TelemetryEvent> with
                    member this.Subscribe observer =
                        calcLoop.Telemetry.Subscribe observer

                interface IDisposable with
                    member this.Dispose () =
                        ()
            }

        let create<'TPolicyRecord, 'TStepResults> (openingPolicyReader, priorClosingStepResultGetter, closingPolicyReader) =
            let calcLoop =
                new CalculationLoop<'TPolicyRecord, 'TStepResults>
                    (Some openingPolicyReader, Some priorClosingStepResultGetter, closingPolicyReader)

            {
                new ICalculationLoop<'TPolicyRecord> with
                    member this.PostAsync (ExitedPolicyId policyId) =
                        calcLoop.PostAsync (CohortedPolicyId.Exited policyId)

                    member this.PostAsync (RemainingPolicyId policyId) =
                        calcLoop.PostAsync (CohortedPolicyId.Remaining policyId)

                    member this.PostAsync (NewPolicyId policyId) =
                        calcLoop.PostAsync (CohortedPolicyId.New policyId)

                interface IObservable<TelemetryEvent> with
                    member this.Subscribe observer =
                        calcLoop.Telemetry.Subscribe observer

                interface IDisposable with
                    member this.Dispose() =
                        ()
            }

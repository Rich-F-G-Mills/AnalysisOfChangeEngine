
namespace AnalysisOfChangeEngine.Controller        


[<AutoOpen>]
module CalculationLoop =

    open System
    open System.Threading
    open System.Threading.Tasks
    open System.Threading.Tasks.Dataflow
    open System.Reactive.Linq
    open System.Reactive.Subjects
    open AnalysisOfChangeEngine
    open AnalysisOfChangeEngine.Controller.CalculationLoop


    let private notifyRequestSubmitted receiver policyId dataSource (RequestorName requestorName) =
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


    type private IProcessedRecordSink<'TPolicyRecord, 'TStepResults> =
        inherit ITargetBlock<ProcessedRecordOutcome<'TPolicyRecord, 'TStepResults>>


    type internal CalculationLoop<'TPolicyRecord, 'TStepResults>
        (openingPolicyGetter: IPolicyGetter<'TPolicyRecord> option,
         priorClosingStepResultGetter: IStepResultsGetter<'TStepResults> option,
         closingPolicyGetter: IPolicyGetter<'TPolicyRecord>) =

            let mutable currentReadIdx = -1
            let mutable currentWriteIdx = -1

            let telemetrySubject =
                new Subject<TelemetryEvent> ()
        
            let policyIdBatcher =
                new BatchBlock<PendingReadRequest> (
                    100,
                    new GroupingDataflowBlockOptions (
                        BoundedCapacity = 1000
                    )
                )

            let policyReader =
                let inner =
                    match openingPolicyGetter, priorClosingStepResultGetter with
                    | Some openingPolicyGetter', Some priorClosingStepResultGetter' ->
                        openingAndClosingReader (openingPolicyGetter', priorClosingStepResultGetter', closingPolicyGetter)
                    | None, None ->
                        closingOnlyReader closingPolicyGetter
                    | _ ->
                        failwith "Unexpected loop parameters."

                fun (pendingReads: PendingReadRequest array) ->
                    backgroundTask {                       
                        let pendingPolicyIds =
                            pendingReads
                            |> Array.map _.PolicyId

                        let readStart =
                            DateTime.Now

                        let! policyRecords =
                            inner pendingPolicyIds

                        let readEnd =
                            DateTime.Now

                        let newReadIdx =
                            Interlocked.Increment &currentReadIdx

                        let readTelemetryEvent =
                            {
                                Idx                         = newReadIdx
                                ReadStart                   = readStart
                                ReadEnd                     = readEnd
                            }

                        do telemetrySubject.OnNext (TelemetryEvent.DataStoreRead readTelemetryEvent)

                        let requestsPendingEvaluation =
                            (pendingReads, policyRecords)
                            ||> Seq.map2 (fun pendingRead ->
                                    Result.map (fun policyRecord ->
                                        {
                                            PolicyId            = pendingRead.PolicyId
                                            PolicyRecord        = policyRecord
                                            RequestSubmitted    = pendingRead.RequestSubmitted
                                        }))

                        return requestsPendingEvaluation
                    }
                    

            let policyReaderBlock =
                new TransformManyBlock<_, _> (
                    policyReader,
                    new ExecutionDataflowBlockOptions (
                        BoundedCapacity = 1000
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
                        calcLoop.PostAsync (PolicyId.New policyId)

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
                        calcLoop.PostAsync (PolicyId.Exited policyId)

                    member this.PostAsync (RemainingPolicyId policyId) =
                        calcLoop.PostAsync (PolicyId.Remaining policyId)

                    member this.PostAsync (NewPolicyId policyId) =
                        calcLoop.PostAsync (PolicyId.New policyId)

                interface IObservable<TelemetryEvent> with
                    member this.Subscribe observer =
                        calcLoop.Telemetry.Subscribe observer

                interface IDisposable with
                    member this.Dispose() =
                        ()
            }

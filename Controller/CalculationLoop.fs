
namespace AnalysisOfChangeEngine.Controller

open System
open System.Threading
open System.Threading.Tasks
open System.Threading.Tasks.Dataflow
open AnalysisOfChangeEngine    
        


module CalculationLoop =

    [<RequireQualifiedAccess>]
    [<NoEquality; NoComparison>]
    type private PolicyId =
        | Exited    of string
        | Remaining of string
        | New       of string

    [<RequireQualifiedAccess>]
    [<NoEquality; NoComparison>]
    type private PolicyRecord<'TPolicyRecord> =
        | Exited    of 'TPolicyRecord
        | Remaining of Opening: 'TPolicyRecord * Closing: 'TPolicyRecord
        | New       of 'TPolicyRecord


    [<NoEquality; NoComparison>]
    type private RecordPendingEvaluation<'TPolicyRecord> =
        {
            PolicyId            : string
            PolicyRecord        : PolicyRecord<'TPolicyRecord>
            RequestSubmitted    : DateTime
        }

    [<RequireQualifiedAccess>]
    [<NoEquality; NoComparison>]
    type private RecordPendingEvaluationFailure =
        | OpeningRecordNotFound
        | ClosingRecordNotFound
        | OpeningRecordParseFailure of Reasons: string array
        | ClosingRecordParseFailure of Reasons: string array

    [<NoEquality; NoComparison>]
    type private RecordPendingEvaluationOutcome<'TPolicyRecord> =
        Result<RecordPendingEvaluation<'TPolicyRecord>, RecordPendingEvaluationFailure>


    [<RequireQualifiedAccess>]
    [<NoEquality; NoComparison>]
    type private ProcessingFailure =
        | PreEvaluationFailure of RecordPendingEvaluationFailure list
        | PostEvaluationFailure of EvaluationFailure list

    [<NoEquality; NoComparison>]
    type ProcessedRecordOutcome<'TPolicyRecord, 'TStepResults> =
        Result<EvaluatedPolicyWalk<'TPolicyRecord, 'TStepResults>, ProcessingFailure>

    [<NoEquality; NoComparison>]
    type private ProcessedRecord<'TPolicyRecord, 'TStepResults> =
        {
            PolicyId            : string
            RequestSubmitted    : DateTime
            Telemetry           : EvaluationTelemetry option
            Outcome             : ProcessedRecordOutcome<'TPolicyRecord, 'TStepResults>
        }

    type private IProcessedRecordSink<'TPolicyRecord, 'TStepResults> =
        inherit ITargetBlock<ProcessedRecord<'TPolicyRecord, 'TStepResults>>


    type internal CalculationLoop<'TPolicyRecord>
        (openingPolicyGetter: IPolicyGetter<'TPolicyRecord> option,
         closingPolicyGetter: IPolicyGetter<'TPolicyRecord>) =
        
            let policyIdBatcher =
                new BatchBlock<_> (
                    100,
                    new GroupingDataflowBlockOptions (
                        BoundedCapacity = 1000
                    )
                )

            let policyReader =
                match openingPolicyGetter with
                | Some openingPolicyGetter' ->
                    fun (policyIds : PolicyId array)  ->
                        backgroundTask {
                            let exitedPolicyIds =
                                policyIds
                                |> Array.choose (
                                    function | PolicyId.Exited pid -> Some pid | _ -> None)

                            let remainingPolicyIds =
                                policyIds
                                |> Array.choose (
                                    function | PolicyId.Remaining pid -> Some pid | _ -> None)

                            let newPolicyIds =
                                policyIds
                                |> Array.choose (
                                    function | PolicyId.New pid -> Some pid | _ -> None)

                            let openingPolicyIds =
                                exitedPolicyIds
                                |> Array.append remainingPolicyIds

                            let closingPolicyIds =
                                newPolicyIds
                                |> Array.append remainingPolicyIds

                            let! openingPolicyRecords =
                                openingPolicyGetter'.GetPolicyRecordsAsync
                                    openingPolicyIds

                            let! closingPolicyRecords =
                                closingPolicyGetter.GetPolicyRecordsAsync
                                    closingPolicyIds

                            let getOpeningPolicyRecord pid =
                                Map.find pid openingPolicyRecords

                            let getClosingPolicyRecord pid =
                                Map.find pid closingPolicyRecords

                            let exitedPolicyRecords =
                                exitedPolicyIds
                                |> Seq.map (fun pid ->
                                    match getOpeningPolicyRecord pid with
                                    | Ok policyRecord ->
                                        Ok (PolicyRecord.Exited policyRecord)
                                    | Error PolicyGetterFailure.NotFound ->
                                        Error [ RecordPendingEvaluationFailure.ClosingRecordNotFound ]
                                    | Error (PolicyGetterFailure.ParseFailure reasons) ->
                                        Error [ RecordPendingEvaluationFailure.ClosingRecordParseFailure reasons ]
                                    | Error PolicyGetterFailure.Cancelled ->
                                        Error [ RecordPendingEvaluationFailure.Cancelled ])

                            let remainingPolicyRecords =
                                remainingPolicyIds
                                |> Seq.map (fun pid ->
                                    let openingRecord =
                                        getOpeningPolicyRecord pid
                                        |> Result.mapError (function
                                            | PolicyGetterFailure.NotFound ->
                                                [ RecordPendingEvaluationFailure.OpeningRecordNotFound ]
                                            | PolicyGetterFailure.ParseFailure reasons ->
                                                [ RecordPendingEvaluationFailure.OpeningRecordParseFailure reasons ]
                                            | PolicyGetterFailure.Cancelled ->
                                    | Ok policyRecord -> 
                                        Ok policyRecord
                                    | Error PolicyGetterFailure.NotFound, Ok _ ->
                                        Error [ RecordPendingEvaluationFailure.OpeningRecordNotFound ]
                                    | Ok _, Error reason) ->
                                        do notifyRecordReadFailure (pid, reason)
                                        None
                                    | Some (Error reason1), Some (Error reason2) ->
                                        do notifyRecordReadFailure (pid, reason1)
                                        do notifyRecordReadFailure (pid, reason2)
                                        None
                                    | _ ->
                                        do notifyRecordNotFound pid
                                        None)

                            let newPolicyRecords =
                                newPolicyIds
                                |> Seq.choose (fun pid ->
                                    match getClosingPolicyRecord pid with
                                    | Some (Ok record) -> 
                                        Some (CohortMembership.New record)
                                    | Some (Error reason) ->
                                        do notifyRecordReadFailure (pid, reason)
                                        None
                                    | None ->
                                        do notifyRecordNotFound pid
                                        None)

                            let combined : CohortedPolicyRecords<'TPolicyRecord> seq =
                                seq {
                                    yield! exitedPolicyRecords
                                    yield! remainingPolicyRecords
                                    yield! newPolicyRecords
                                }

                            return combined
                        }
                | None ->
                    fun policyIds ->
                        backgroundTask {
                            let newPolicyIds =
                                policyIds
                                |> Array.choose (function
                                    | CohortMembership.New pid -> Some pid 
                                    | _ -> failwith "Unexpected cohort membership type.")

                            let! closingPolicyRecords =
                                closingPolicyGetter.GetPolicyRecordsAsync
                                    newPolicyIds

                            let getClosingPolicyRecord pid =
                                Map.find pid closingPolicyRecords

                            let newPolicyRecords =
                                newPolicyIds
                                |> Seq.choose (fun pid ->
                                    match getClosingPolicyRecord pid with
                                    | Some (Ok record) -> 
                                        Some (CohortMembership.Exited record)
                                    | Some (Error reason) ->
                                        do notifyRecordReadFailure (pid, reason)
                                        None
                                    | None ->
                                        do notifyRecordNotFound pid
                                        None)

                            return newPolicyRecords
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

            let batchLink =
                pendingBufferBlock.LinkTo (policyIdBatcher, linkOptions)

            let readerLink =
                policyIdBatcher.LinkTo (policyReaderBlock, linkOptions)


            member internal _.PostAsync (policyId) =
                pendingBufferBlock.SendAsync (policyId)


    type INewOnlyCalculationLoop<'TPolicyRecord> =
        inherit IDisposable

        abstract member PostAsync: NewPolicyId          -> Task<bool>

    type ICalculationLoop<'TPolicyRecord> =
        inherit INewOnlyCalculationLoop<'TPolicyRecord>
        inherit IDisposable

        abstract member PostAsync: ExitedPolicyId       -> Task<bool>
        abstract member PostAsync: RemainingPolicyId    -> Task<bool>        


    [<RequireQualifiedAccess>]
    module CalculationLoop =
    
        let createClosingOnly (closingPolicyReader) =
            let calcLoop =
                new CalculationLoop<'TPolicyRecord>
                    (None, closingPolicyReader)

            {
                new INewOnlyCalculationLoop<'TPolicyRecord> with
                    member this.PostAsync (NewPolicyId policyId) =
                        calcLoop.PostAsync (CohortMembership.New policyId)

                interface IDisposable with
                    member this.Dispose() =
                        ()
            }

        let create (openingPolicyReader, closingPolicyReader) =
            let calcLoop =
                new CalculationLoop<'TPolicyRecord>
                    (Some openingPolicyReader, closingPolicyReader)

            {
                new ICalculationLoop<'TPolicyRecord> with
                    member this.PostAsync (ExitedPolicyId policyId) =
                        calcLoop.PostAsync (CohortMembership.Exited policyId)

                    member this.PostAsync (RemainingPolicyId policyId) =
                        calcLoop.PostAsync (CohortMembership.Remaining policyId)

                    member this.PostAsync (NewPolicyId policyId) =
                        calcLoop.PostAsync (CohortMembership.New policyId)

                interface IDisposable with
                    member this.Dispose() =
                        ()
            }


    *)

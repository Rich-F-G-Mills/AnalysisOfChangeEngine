
namespace AnalysisOfChangeEngine.Controller

open System
open System.Threading
open System.Threading.Tasks
open System.Threading.Tasks.Dataflow
open AnalysisOfChangeEngine    
        

(*

[<RequireQualifiedAccess>]
type internal CohortMembership<'T, 'U> =
    | Exited of 'T
    | Remaining of 'U
    | New of 'T


type internal CohortedPolicyIds =
    CohortMembership<string, string>

type internal CohortedPolicyRecords<'TPolicyRecord> =
    CohortMembership<'TPolicyRecord, 'TPolicyRecord * 'TPolicyRecord>


type internal CalculationLoop<'TPolicyRecord>
    (openingPolicyGetter: IPolicyGetter<'TPolicyRecord> option,
     closingPolicyGetter: IPolicyGetter<'TPolicyRecord>) =
        
        let pendingBufferBlock =
            new BufferBlock<_> (
                new DataflowBlockOptions (
                    BoundedCapacity = 1000
                )
            )

        let policyIdBatcher =
            new BatchBlock<_> (
                100,
                new GroupingDataflowBlockOptions (
                    BoundedCapacity = 1000
                )
            )

        let notifyRecordNotFound pid = ()

        let notifyRecordReadFailure (pid, reason) = ()

        let policyReader =
            match openingPolicyGetter with
            | Some openingPolicyGetter' ->
                fun (policyIds : CohortedPolicyIds array)  ->
                    backgroundTask {
                        let exitedPolicyIds =
                            policyIds
                            |> Array.choose (
                                function | CohortMembership.Exited pid -> Some pid | _ -> None)

                        let remainingPolicyIds =
                            policyIds
                            |> Array.choose (
                                function | CohortMembership.Remaining pid -> Some pid | _ -> None)

                        let newPolicyIds =
                            policyIds
                            |> Array.choose (
                                function | CohortMembership.New pid -> Some pid | _ -> None)

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
                            |> Seq.choose (fun pid ->
                                match getOpeningPolicyRecord pid with
                                | Ok record ->
                                    Some (ExitedPolicy record)
                                | Error (PolicyGetterFailure.ParseFailure reason) ->
                                    do notifyRecordReadFailure (pid, reason)
                                    None
                                | Error PolicyGetterFailure.NotFound ->
                                    do notifyRecordNotFound pid
                                    None
                                | Error PolicyGetterFailure.Cancelled ->
                                    None)

                        let remainingPolicyRecords =
                            remainingPolicyIds
                            |> Seq.choose (fun pid ->
                                match getOpeningPolicyRecord pid, getClosingPolicyRecord pid with
                                | Some (Ok openingRecord), Some (Ok closingRecord) -> 
                                    Some (CohortMembership.Remaining (openingRecord, closingRecord))
                                | Some (Error reason), Some (Ok _)
                                | Some (Ok _), Some (Error reason) ->
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


namespace AnalysisOfChangeEngine.Controller.CalculationLoop


[<AutoOpen>]
module internal PolicyReader =

    open FsToolkit.ErrorHandling
    open AnalysisOfChangeEngine.Common
    open AnalysisOfChangeEngine.Controller


    let internal openingAndClosingReader
        (openingPolicyGetter: IPolicyGetter<'TPolicyRecord>,
         priorClosingStepResultGetter: IStepResultsGetter<'TStepResults>,
         closingPolicyGetter: IPolicyGetter<'TPolicyRecord>)
        policyIds =
            backgroundTask {
                let exitedPolicyIds =
                    policyIds
                    |> Array.choose (function
                        | CohortedPolicyId.Exited pid -> Some pid | _ -> None)

                let remainingPolicyIds =
                    policyIds
                    |> Array.choose (function
                        | CohortedPolicyId.Remaining pid -> Some pid | _ -> None)

                let newPolicyIds =
                    policyIds
                    |> Array.choose (function
                        | CohortedPolicyId.New pid -> Some pid | _ -> None)                

                let openingPolicyIds =
                    exitedPolicyIds
                    |> Array.append remainingPolicyIds

                let closingPolicyIds =
                    newPolicyIds
                    |> Array.append remainingPolicyIds

                let! openingPolicyRecords =
                    openingPolicyGetter.GetPolicyRecordsAsync
                        openingPolicyIds

                let! closingPolicyRecords =
                    closingPolicyGetter.GetPolicyRecordsAsync
                        closingPolicyIds

                let! priorClosingStepResults =
                    priorClosingStepResultGetter.GetStepResultsAsync
                        openingPolicyIds
                            
                let getOpeningPolicyRecord pid =
                    match Map.tryFind pid openingPolicyRecords with
                    | None ->
                        Error (nonEmptyList { yield PolicyReadFailure.OpeningRecordNotFound })
                    | Some (Ok record) ->
                        Ok record
                    | Some (Error readerFailures) ->
                        Error (nonEmptyList { yield PolicyReadFailure.OpeningRecordReadFailure readerFailures })

                let getClosingPolicyRecord pid =
                    match Map.tryFind pid closingPolicyRecords with
                    | None ->
                        Error (nonEmptyList { yield PolicyReadFailure.ClosingRecordNotFound })
                    | Some (Ok record) ->
                        Ok record
                    | Some (Error readerFailures) ->
                        Error (nonEmptyList { yield PolicyReadFailure.ClosingRecordReadFailure readerFailures })

                let getPriorClosingStepResult pid =
                    match Map.tryFind pid priorClosingStepResults with
                    | None ->
                        Ok None
                    | Some (Ok stepResults) ->
                        Ok (Some stepResults)
                    | Some (Error failure) ->
                        Error (nonEmptyList { yield PolicyReadFailure.PriorClosingStepResultsReadFailure failure })

                let exitedPolicyRecords =
                    exitedPolicyIds
                    |> Seq.map (fun pid ->
                        let openingRecord =
                            getOpeningPolicyRecord pid

                        let priorClosingStepResults =
                            getPriorClosingStepResult pid

                        match openingRecord, priorClosingStepResults with
                        | Ok policyRecord, Ok priorClosingStepResults' ->
                            Ok (CohortedPolicyRecord.Exited (policyRecord, priorClosingStepResults'))
                        | Error failures, Ok _
                        | Ok _, Error failures ->
                            Error failures
                        | Error failures1, Error failures2 ->
                            Error (failures1 .@ failures2))
                    |> Seq.zip exitedPolicyIds

                let remainingPolicyRecords =
                    remainingPolicyIds
                    |> Seq.map (fun pid ->
                        let openingPolicyRecord =
                            getOpeningPolicyRecord pid
                                            
                        let closingPolicyRecord =
                            getClosingPolicyRecord pid

                        let priorClosingStepResult =
                            getPriorClosingStepResult pid

                        // TODO - There is undoubtedly a better way of doing this. However,
                        // having to combine multiple non-empty lists does add _some_ complexity.
                        match openingPolicyRecord, priorClosingStepResult, closingPolicyRecord with
                        | Ok openingPolicyRecord', Ok priorClosingStepResult', Ok closingPolicyRecord' ->
                            Ok (CohortedPolicyRecord.Remaining (openingPolicyRecord', closingPolicyRecord', priorClosingStepResult'))
                        | Error failures, Ok _, Ok _
                        | Ok _, Error failures, Ok _
                        | Ok _, Ok _, Error failures ->
                            Error failures
                        | Error failures1, Error failures2, Ok _
                        | Error failures1, Ok _, Error failures2
                        | Ok _, Error failures1, Error failures2 ->
                            Error (failures1 .@ failures2)
                        | Error failures1, Error failures2, Error failures3 ->
                            Error (failures1 .@ failures2 .@ failures3))
                    |> Seq.zip remainingPolicyIds

                let newPolicyRecords =
                    newPolicyIds
                    |> Seq.map (getClosingPolicyRecord >> Result.map CohortedPolicyRecord.New)
                    |> Seq.zip newPolicyIds

                let combinedRequests =
                    exitedPolicyRecords
                    |> Seq.append remainingPolicyRecords
                    |> Seq.append newPolicyRecords
                    |> Map.ofSeq

                return
                    policyIds
                    |> Array.map (fun pid -> combinedRequests[pid.Underlying])
            }

    let internal closingOnlyReader
        (closingPolicyGetter: IPolicyGetter<'TPolicyRecord>) newPolicyIds =
            backgroundTask {
                let newPolicyIds' =
                    newPolicyIds
                    |> Array.map (function
                        // We don't use underlying as we need to ensure that we only get new records.
                        | CohortedPolicyId.New pid  -> pid
                        | _                         -> failwith "Unexpected cohort.")

                let! closingPolicyRecords =
                    closingPolicyGetter.GetPolicyRecordsAsync
                        newPolicyIds'

                let getClosingPolicyRecord pid =
                    match Map.tryFind pid closingPolicyRecords with
                    | None ->
                        Error (nonEmptyList { yield  PolicyReadFailure.ClosingRecordNotFound })
                    | Some (Ok record) ->
                        Ok (CohortedPolicyRecord.New record)
                    | Some (Error readerFailure) ->
                        Error (nonEmptyList { yield PolicyReadFailure.ClosingRecordReadFailure readerFailure })

                let newPolicyRecords =
                    newPolicyIds'
                    |> Array.map getClosingPolicyRecord

                return newPolicyRecords
            }

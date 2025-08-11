
namespace AnalysisOfChangeEngine.Controller.CalculationLoop


[<AutoOpen>]
module internal PolicyReader =

    open FsToolkit.ErrorHandling
    open AnalysisOfChangeEngine.Controller


    let private combineErrors x y =
        Result.either (fun _ -> List.empty) id x
        |> List.append (Result.either (fun _ -> List.empty) id y)
        |> Error


    let internal openingAndClosingReader<'TPolicyRecord, 'TStepResults>
        (openingPolicyGetter: IPolicyGetter<'TPolicyRecord>,
         priorClosingStepResultGetter: IStepResultsGetter<'TStepResults>,
         closingPolicyGetter: IPolicyGetter<'TPolicyRecord>)
        policyIds =
            backgroundTask {
                let exitedPolicyIds =
                    policyIds
                    |> Array.choose (function | CohortedPolicyId.Exited pid -> Some pid | _ -> None)

                let remainingPolicyIds =
                    policyIds
                    |> Array.choose (function | CohortedPolicyId.Remaining pid -> Some pid | _ -> None)

                let newPolicyIds =
                    policyIds
                    |> Array.choose (function | CohortedPolicyId.New pid -> Some pid | _ -> None)                

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
                    Map.find pid openingPolicyRecords

                let getClosingPolicyRecord pid =
                    Map.find pid closingPolicyRecords

                let getPriorClosingStepResult pid =
                    Map.tryFind pid priorClosingStepResults                           

                let exitedPolicyRecords =
                    exitedPolicyIds
                    |> Seq.map (fun pid ->
                        let openingRecord =
                            getOpeningPolicyRecord pid
                            |> Result.mapError (function
                                | PolicyGetterFailure.NotFound ->
                                    [ PolicyReadFailure.OpeningRecordNotFound ]
                                | (PolicyGetterFailure.ParseFailure reasons) ->
                                    [ PolicyReadFailure.OpeningRecordParseFailure reasons ])

                        let priorClosingStepResults =
                            match getPriorClosingStepResult pid with
                            | Some (Ok stepResults) ->
                                Ok (Some stepResults)
                            | Some (Error (StepResultsGetterFailure.ParseFailure reasons)) ->
                                Error [ PolicyReadFailure.PriorClosingStepResultsParseFailure reasons ]
                            | None ->
                                Ok None

                        match openingRecord, priorClosingStepResults with
                        | Ok policyRecord, Ok priorClosingStepResults' ->
                            Ok (CohortedPolicyRecord.Exited (policyRecord, priorClosingStepResults'))
                        // TODO - Could replace this with the combine errors function.
                        | Error failures1, Error failures2 ->
                            Error (failures1 @ failures2)
                        | Error failures1, Ok _ ->
                            Error failures1
                        | Ok _, Error failures2 ->
                            Error failures2)
                    |> Seq.zip exitedPolicyIds

                let remainingPolicyRecords =
                    remainingPolicyIds
                    |> Seq.map (fun pid ->
                        let openingPolicyRecord =
                            getOpeningPolicyRecord pid
                            |> Result.mapError (function
                                | PolicyGetterFailure.NotFound ->
                                    [ PolicyReadFailure.OpeningRecordNotFound ]
                                | PolicyGetterFailure.ParseFailure reasons ->
                                    [ PolicyReadFailure.OpeningRecordParseFailure reasons ])
                                            
                        let closingPolicyRecord =
                            getClosingPolicyRecord pid
                            |> Result.mapError (function
                                | PolicyGetterFailure.NotFound ->
                                    [ PolicyReadFailure.ClosingRecordNotFound ]
                                | PolicyGetterFailure.ParseFailure reasons ->
                                    [ PolicyReadFailure.ClosingRecordParseFailure reasons ])

                        let priorClosingStepResult =
                            match getPriorClosingStepResult pid with
                            | Some (Ok stepResults) ->
                                Ok (Some stepResults)
                            | Some (Error (StepResultsGetterFailure.ParseFailure reasons)) ->
                                Error [ PolicyReadFailure.PriorClosingStepResultsParseFailure reasons ]
                            | None ->
                                Ok None

                        // TODO - There is undou 
                        match openingPolicyRecord, priorClosingStepResult, closingPolicyRecord with
                        | Ok openingPolicyRecord', Ok priorClosingStepResult', Ok closingPolicyRecord' ->
                            Ok (CohortedPolicyRecord.Remaining (openingPolicyRecord', closingPolicyRecord', priorClosingStepResult'))
                        | _ ->
                            Error List.empty
                            |> combineErrors openingPolicyRecord
                            |> combineErrors priorClosingStepResult
                            |> combineErrors closingPolicyRecord)
                    |> Seq.zip remainingPolicyIds

                let newPolicyRecords =
                    newPolicyIds
                    |> Seq.map (fun pid ->
                        match getClosingPolicyRecord pid with
                        | Ok policyRecord ->
                            Ok (CohortedPolicyRecord.New policyRecord)
                        | Error PolicyGetterFailure.NotFound ->
                            Error [ PolicyReadFailure.ClosingRecordNotFound ]
                        | Error (PolicyGetterFailure.ParseFailure reasons) ->
                            Error [ PolicyReadFailure.ClosingRecordParseFailure reasons ])
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

    let internal closingOnlyReader<'TPolicyRecord, 'TStepResults>
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
                    Map.find pid closingPolicyRecords                                        

                let newPolicyRecords =
                    newPolicyIds'
                    |> Array.map (fun pid ->
                        match getClosingPolicyRecord pid with
                        | Ok policyRecord ->
                            // Without the type hint, we cannot infer the step results type.
                            Ok (CohortedPolicyRecord<'TPolicyRecord, 'TStepResults>.New policyRecord)
                        | Error PolicyGetterFailure.NotFound ->
                            Error [ PolicyReadFailure.ClosingRecordNotFound ]
                        | Error (PolicyGetterFailure.ParseFailure reasons) ->
                            Error [ PolicyReadFailure.ClosingRecordParseFailure reasons ])

                return newPolicyRecords
            }


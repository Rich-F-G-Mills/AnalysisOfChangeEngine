
namespace AnalysisOfChangeEngine.Controller.CalculationLoop


[<AutoOpen>]
module internal PolicyReader =

    open AnalysisOfChangeEngine


    let internal openingAndClosingReader<'TPolicyRecord, 'TStepResults>
        (openingPolicyGetter: IPolicyGetter<'TPolicyRecord>,
         priorClosingStepResultGetter: IStepResultsGetter<'TStepResults>,
         closingPolicyGetter: IPolicyGetter<'TPolicyRecord>)
        policyIds =
            backgroundTask {
                let exitedPolicyIds =
                    policyIds
                    |> Array.choose (function | PolicyId.Exited pid -> Some pid | _ -> None)

                let remainingPolicyIds =
                    policyIds
                    |> Array.choose (function | PolicyId.Remaining pid -> Some pid | _ -> None)

                let newPolicyIds =
                    policyIds
                    |> Array.choose (function | PolicyId.New pid -> Some pid | _ -> None)                

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
                        match getOpeningPolicyRecord pid with
                        | Ok policyRecord ->
                            Ok (PolicyRecord.Exited (policyRecord, getPriorClosingStepResult pid))
                        | Error PolicyGetterFailure.NotFound ->
                            Error [ PendingEvaluationRequestFailure.OpeningRecordNotFound ]
                        | Error (PolicyGetterFailure.ParseFailure reasons) ->
                            Error [ PendingEvaluationRequestFailure.OpeningRecordParseFailure reasons ])
                    |> Seq.zip exitedPolicyIds

                let remainingPolicyRecords =
                    remainingPolicyIds
                    |> Seq.map (fun pid ->
                        let openingPolicyRecord =
                            match getOpeningPolicyRecord pid with
                            | Ok openingRecord ->
                                Ok openingRecord
                            | Error PolicyGetterFailure.NotFound ->
                                Error [ PendingEvaluationRequestFailure.OpeningRecordNotFound ]
                            | Error (PolicyGetterFailure.ParseFailure reasons) ->
                                Error [ PendingEvaluationRequestFailure.OpeningRecordParseFailure reasons ]
                                            
                        let closingPolicyRecord =
                            match getClosingPolicyRecord pid with
                            | Ok openingRecord ->
                                Ok openingRecord
                            | Error PolicyGetterFailure.NotFound ->
                                Error [ PendingEvaluationRequestFailure.ClosingRecordNotFound ]
                            | Error (PolicyGetterFailure.ParseFailure reasons) ->
                                Error [ PendingEvaluationRequestFailure.ClosingRecordParseFailure reasons ]

                        let priorClosingStepResult =
                            getPriorClosingStepResult pid

                        match openingPolicyRecord, closingPolicyRecord with
                        | Ok openingPolicyRecord', Ok closingPolicyRecord' ->
                            Ok (PolicyRecord.Remaining (openingPolicyRecord', closingPolicyRecord', priorClosingStepResult))
                        | Error openingFailures, Ok _ ->
                            Error openingFailures
                        | Ok _, Error closingFailures ->
                            Error closingFailures
                        | Error openingFailures, Error closingFailures ->
                            Error (openingFailures @ closingFailures))
                    |> Seq.zip remainingPolicyIds

                let newPolicyRecords =
                    newPolicyIds
                    |> Seq.map (fun pid ->
                        match getClosingPolicyRecord pid with
                        | Ok policyRecord ->
                            Ok (PolicyRecord.New policyRecord)
                        | Error PolicyGetterFailure.NotFound ->
                            Error [ PendingEvaluationRequestFailure.ClosingRecordNotFound ]
                        | Error (PolicyGetterFailure.ParseFailure reasons) ->
                            Error [ PendingEvaluationRequestFailure.ClosingRecordParseFailure reasons ])
                    |> Seq.zip newPolicyIds

                let combinedRequests =
                    exitedPolicyRecords
                    |> Seq.append remainingPolicyRecords
                    |> Seq.append newPolicyRecords
                    |> Map.ofSeq

                return
                    policyIds
                    |> Array.map (fun pid -> combinedRequests[pid.PolicyId])
            }

    let internal closingOnlyReader<'TPolicyRecord, 'TStepResults>
        (closingPolicyGetter: IPolicyGetter<'TPolicyRecord>) newPolicyIds =
            backgroundTask {
                let newPolicyIds' =
                    newPolicyIds
                    |> Array.map (function
                        | PolicyId.New pid -> 
                            pid
                        | _ ->
                            failwith "Unexpected cohort.")

                let! closingPolicyRecords =
                    closingPolicyGetter.GetPolicyRecordsAsync
                        newPolicyIds'

                let getClosingPolicyRecord pid =
                    Map.find pid closingPolicyRecords                                        

                // Without the type hint, we cannot infer the step results type.
                let newPolicyRecords : Result<PolicyRecord<_, 'TStepResults>, _> array =
                    newPolicyIds'
                    |> Array.map (fun pid ->
                        match getClosingPolicyRecord pid with
                        | Ok policyRecord ->
                            Ok (PolicyRecord.New policyRecord)
                        | Error PolicyGetterFailure.NotFound ->
                            Error [ PendingEvaluationRequestFailure.ClosingRecordNotFound ]
                        | Error (PolicyGetterFailure.ParseFailure reasons) ->
                            Error [ PendingEvaluationRequestFailure.ClosingRecordParseFailure reasons ])

                return newPolicyRecords
            }


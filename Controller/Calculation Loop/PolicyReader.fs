
namespace AnalysisOfChangeEngine.Controller.CalculationLoop


[<AutoOpen>]
module internal PolicyReader =

    open AnalysisOfChangeEngine.Controller


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
                        match getOpeningPolicyRecord pid with
                        | Ok policyRecord ->
                            Ok (CohortedPolicyRecord.Exited (policyRecord, getPriorClosingStepResult pid))
                        | Error PolicyGetterFailure.NotFound ->
                            Error [ PolicyReadFailure.OpeningRecordNotFound ]
                        | Error (PolicyGetterFailure.ParseFailure reasons) ->
                            Error [ PolicyReadFailure.OpeningRecordParseFailure reasons ])
                    |> Seq.zip exitedPolicyIds

                let remainingPolicyRecords =
                    remainingPolicyIds
                    |> Seq.map (fun pid ->
                        let openingPolicyRecord =
                            match getOpeningPolicyRecord pid with
                            | Ok openingRecord ->
                                Ok openingRecord
                            | Error PolicyGetterFailure.NotFound ->
                                Error [ PolicyReadFailure.OpeningRecordNotFound ]
                            | Error (PolicyGetterFailure.ParseFailure reasons) ->
                                Error [ PolicyReadFailure.OpeningRecordParseFailure reasons ]
                                            
                        let closingPolicyRecord =
                            match getClosingPolicyRecord pid with
                            | Ok openingRecord ->
                                Ok openingRecord
                            | Error PolicyGetterFailure.NotFound ->
                                Error [ PolicyReadFailure.ClosingRecordNotFound ]
                            | Error (PolicyGetterFailure.ParseFailure reasons) ->
                                Error [ PolicyReadFailure.ClosingRecordParseFailure reasons ]

                        let priorClosingStepResult =
                            getPriorClosingStepResult pid

                        match openingPolicyRecord, closingPolicyRecord with
                        | Ok openingPolicyRecord', Ok closingPolicyRecord' ->
                            Ok (CohortedPolicyRecord.Remaining (openingPolicyRecord', closingPolicyRecord', priorClosingStepResult))
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


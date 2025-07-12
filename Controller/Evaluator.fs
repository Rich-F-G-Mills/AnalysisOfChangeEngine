
namespace AnalysisOfChangeEngine.Controller


[<RequireQualifiedAccess>]
module Evaluator =

    open System.Threading.Tasks
    open FsToolkit.ErrorHandling
    open AnalysisOfChangeEngine
    open AnalysisOfChangeEngine.Controller.WalkAnalyser


    // We need to move from the world of API failures into the world of
    // failed evaluations.
    let private failureMapper (requestor: AbstractApiRequestor<_>) = function
        | ApiRequestFailure.CalculationFailure reasons ->
            EvaluationFailure.ApiCalculationFailure (requestor.Name, reasons)
        | ApiRequestFailure.CallFailure reasons ->
            EvaluationFailure.ApiCallFailure (requestor.Name, reasons)
        | ApiRequestFailure.Cancelled ->
            EvaluationFailure.Cancelled


    // We need to at least give a type hint for the abstract requestor, otherwise
    // we cannot access the 'Name' property.
    let private apiResponsesConsolidator
        (acc: Result<Map<AbstractApiRequestor<_>, _>, _>)
        (apiRequstor, apiResponse) =
            match acc, apiRequstor, apiResponse with
            // Once we've been cancelled, we don't care about anything else.
            | _, _, Error ApiRequestFailure.Cancelled
            | Error [ EvaluationFailure.Cancelled ], _, _ ->
                Error [ EvaluationFailure.Cancelled ]
            // No prior failures and current requestor was successful.
            | Ok acc', requestor, Ok results ->
                Ok (acc' |> Map.add requestor results)
            // If we've already encountered a failure, we don't care
            // about any subsequent successes. ie... We're doomed to
            // failure at this point!
            | Error _, _, Ok _ ->
                acc
            // Have just received our first error.
            | Ok _, requestor, Error failure ->
                Error [ failureMapper requestor failure ]
            // Have just recieved yet another (!) error.
            | Error failures, requestor, Error failure ->
                Error ((failureMapper requestor failure)::failures)


    let private createSingleStepEvaluator<'TPolicyRecord, 'TStepResults, 'TApiCollection>
        (parsedSource: ParsedSource<'TPolicyRecord, 'TStepResults>) =
            let apiCalls, invoker =
                // Don't forget that F# sets are ordered. Iterating over an unordered set
                // could well lead to palpatations!
                parsedSource.ApiCalls, parsedSource.Invoker           

            // Group our API outputs by their respective requestors.
            let groupedDependenciesByRequestor =
                apiCalls
                |> Seq.groupBy _.Requestor
                |> Seq.map (fun (requestor, dependencies) ->
                    requestor, Seq.toArray dependencies)
                |> Map.ofSeq
                
            // For our grouped API outputs, create a wrapper that, when
            // supplied the policy record, will return a corresponding task
            // for the requested outputs.
            let nestedApiCallTasksByRequestor =
                groupedDependenciesByRequestor
                |> Map.map (fun requestor dependencies ->
                    let outputs =
                        dependencies
                        |> Array.map _.OutputProperty

                    let asyncExecutor =
                        requestor.ExecuteAsync outputs

                    asyncExecutor)

            // For each API output that our step invoker is expecting, create a function
            // that can take a set of mapped API 'outcomes' and extract the relevant bit.
            let resultMappers =
                apiCalls
                |> Seq.map (fun dependency ->
                    let nestedIdxWithinRequestor =
                        groupedDependenciesByRequestor[dependency.Requestor]
                        |> Array.findIndex _.Equals(dependency)

                    fun (apiResponses: Map<_, obj array>) ->
                        let responsesWithinRequestor =
                            apiResponses[dependency.Requestor]

                        responsesWithinRequestor[nestedIdxWithinRequestor])
                |> Seq.toArray

            let responseArrayHydrator responsesByRequestor idx =
                resultMappers[idx] responsesByRequestor

            fun policyRecord ->
                backgroundTask {
                    let responseTasksByRequestor =
                        nestedApiCallTasksByRequestor
                        |> Map.map (fun _ asyncExecutor -> asyncExecutor policyRecord)
                            
                    // Allows us to asynchronously await all tasks without using
                    // a blocking Wait command.
                    let! _ =
                        responseTasksByRequestor
                        |> Map.values
                        |> Task.WhenAll

                    // Given we've already awaited the tasks above, ths should
                    // be a non-blocking operation.
                    // TODO - For now, we'll assume that all tasks complete in
                    // a non-cancelled, non-faulted state.
                    let responsesByRequestor =
                        responseTasksByRequestor
                        |> Map.map (fun _ -> _.Result)

                    let consolidatedResponses =
                        responsesByRequestor
                        |> Map.toSeq
                        |> Seq.fold apiResponsesConsolidator (Ok Map.empty)  
                            
                    let evaluationOutcome =
                        match consolidatedResponses with
                        | Ok resultsByRequestor ->
                            let apiResults =
                                Array.init apiCalls.Count (responseArrayHydrator resultsByRequestor)

                            // Now we have the required API outputs, we can pass these
                            // on to our step logic.
                            Ok (invoker (policyRecord, apiResults))

                        | Error failures ->
                            // Necessary as we're mapping from one result domain to another.
                            Error failures

                    return evaluationOutcome
                }


    let private createRemainingPolicyEvaluator<'TPolicyRecord, 'TStepResults, 'TApiCollection>
        (parsedWalk: ParsedWalk<'TPolicyRecord, 'TStepResults>) =

            (*
            Design Decision:
                For a given record, we could process one data stage at a time.
                However, if a data stage is going to fail, it is better we find
                this out as soon as possible so we don't waste time making
                API calls that will just be discarded anyway!
            *)
            
            fun (RemainingPolicy (openingPolicyRecord, closingPolicyRecord)) ->
                result {
                    let rec extractDataChanges priorPolicyRecord
                        : PostOpeningDataStage<'TPolicyRecord, 'TStepResults> list -> _ = function
                        | ds::dss ->
                            let newRecordForDataStage =
                                ds.DataChangeStep.DataChanger
                                    (openingPolicyRecord, priorPolicyRecord, closingPolicyRecord)
                                        
                            match newRecordForDataStage with
                            | Ok None ->
                                Result.bind
                                    (fun subsequentStages -> Ok (None::subsequentStages))
                                    // If the current data change step hasn't changed anything,
                                    // then we'll just carry forward our inherited view of the
                                    // latest policy record.
                                    (extractDataChanges priorPolicyRecord dss)
                            | Ok (Some newRecord as newRecord') ->
                                Result.bind
                                    (fun subsequentStages -> Ok (newRecord'::subsequentStages))
                                    // However, if the data change step _does_ lead to a new record,
                                    // then that will need to be carried forward instead.
                                    (extractDataChanges newRecord dss)
                            | Error reason ->
                                // If we encounter an error... Then we can immediately return.
                                Error (EvaluationFailure.DataChangeFailure
                                    (ds.DataChangeStep, [| reason |]))
                        | [] ->
                            Ok []                                   

                    // Note that this will exclude the opening policy record itself.
                    // If, for example, this was a list of None's, that'd suggest that
                    // the opening record and closing records are the same.
                    let! policyRecordDataChanges =
                        extractDataChanges openingPolicyRecord parsedWalk.PostOpeningDataStages

                    assert (policyRecordDataChanges.Length = parsedWalk.PostOpeningDataStages.Length)



                    return 0
                }
                


    let private createExitedPolicyEvaluator<'TPolicyRecord, 'TStepResults, 'TApiCollection>
        (parsedWalk: ParsedWalk<'TPolicyRecord, 'TStepResults>) =
            let openingReRunStepHdr, openingReRunParsedStep =
                // This CANNOT fail! If it does, I've not idea
                // how the logic managed to get this far!
                List.head parsedWalk.ParsedSteps

            assert checkStepType<OpeningReRunStep<'TPolicyRecord, 'TStepResults, 'TApiCollection>> openingReRunStepHdr

            let evaluator =
                createSingleStepEvaluator openingReRunParsedStep

            fun (ExitedPolicy policyRecord) ->
                evaluator policyRecord

    let private createNewPolicyEvaluator<'TPolicyRecord, 'TStepResults, 'TApiCollection>
        (parsedWalk: ParsedWalk<'TPolicyRecord, 'TStepResults>) =
            let newRecordsStepHdr, newRecordsParsedStep =
                // This CANNOT fail! If it does, I've not idea
                // how the logic managed to get this far!
                List.last parsedWalk.ParsedSteps

            assert checkStepType<AddNewRecordsStep<'TPolicyRecord, 'TStepResults>> newRecordsStepHdr

            let evaluator =
                createSingleStepEvaluator newRecordsParsedStep

            fun (NewPolicy policyRecord) ->
                evaluator policyRecord

    let create (walk: AbstractWalk<'TPolicyRecord, 'TStepResults, 'TApiCollection>) apiCollection =
        let parsedWalk =
            WalkParser.execute walk apiCollection

        let exitedRecordEvaluator =
            // If we don't specificy the API collection type, obj will be inferred
            // which will lead to a type assertion failure at runtime.
            createExitedPolicyEvaluator<_, _, 'TApiCollection> parsedWalk

        let newRecordEvaluator =
            createNewPolicyEvaluator<_, _, 'TApiCollection> parsedWalk

        {
            new IPolicyEvaluator<'TPolicyRecord, 'TStepResults> with

                member _.Execute (ExitedPolicy _ as exitedPolicyRecord) =
                    exitedRecordEvaluator exitedPolicyRecord

                member _.Execute (NewPolicy _ as newPolicyRecord) =
                    newRecordEvaluator newPolicyRecord
        }






                
                


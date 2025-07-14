
namespace AnalysisOfChangeEngine.Controller


[<RequireQualifiedAccess>]
module Evaluator =

    open System
    open System.Collections.Concurrent
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
        (apiRequstor, (apiResponse, _)) =
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


    let private createDataStageEvaluator<'TPolicyRecord, 'TStepResults, 'TApiCollection>
        (parsedSources: ParsedSource<'TPolicyRecord, 'TStepResults> list) =
            let apiDependencies =
                parsedSources
                |> List.map _.ApiCalls

            let combinedApiDependencies =
                apiDependencies
                |> Set.unionMany

            let groupedDependenciesByRequestor =
                combinedApiDependencies
                |> Seq.groupBy _.Requestor
                |> Seq.map (fun (requestor, dependencies) ->
                    requestor, Seq.toArray dependencies)
                |> Map.ofSeq

            let deferredApiCallTasksByRequestor =
                groupedDependenciesByRequestor
                |> Map.map (fun requestor dependencies ->
                    let outputs =
                        dependencies
                        |> Array.map _.OutputProperty

                    let asyncExecutor =
                        requestor.ExecuteAsync outputs

                    asyncExecutor)

            // Execute all API calls within a given group.
            let groupedDependencyExecutor policyRecord =
                deferredApiCallTasksByRequestor
                |> Map.map (fun _ asyncExecutor -> asyncExecutor policyRecord)

            let resultMappers =
                apiDependencies
                |> List.map (fun stepDependencies ->
                    stepDependencies
                    |> Seq.map (fun dependency ->
                        let nestedIdxWithinRequestor =
                            groupedDependenciesByRequestor[dependency.Requestor]
                            |> Array.findIndex _.Equals(dependency)

                        fun (apiResponses: Map<_, obj array>) ->
                            let responsesWithinRequestor =
                                apiResponses[dependency.Requestor]

                            responsesWithinRequestor[nestedIdxWithinRequestor])
                    |> Seq.toArray)

            let responseArrayHydrators =
                resultMappers
                |> List.map (fun mapper responsesByRequestor idx ->
                    mapper[idx] responsesByRequestor)

            fun policyRecord ->
                backgroundTask {
                    let responseTasksByRequestor =
                        groupedDependencyExecutor policyRecord
                            
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
                            let stepResults =
                                (parsedSources, responseArrayHydrators)
                                ||> List.map2 (fun source hydrator ->
                                        let invokerArgs =
                                            Array.init source.ApiCalls.Count (hydrator resultsByRequestor)

                                        let stepResult =
                                            source.Invoker (policyRecord, invokerArgs)
                                    
                                        stepResult)

                            Ok stepResults

                        | Error failures ->
                            // Necessary as we're mapping from one result domain to another.
                            Error failures

                    let apiRequestsTelemetry =
                        responsesByRequestor
                        |> Map.values
                        |> Seq.map snd
                        |> Seq.toList

                    return evaluationOutcome, apiRequestsTelemetry
                }

    (*
    let private createEvaluatorsForProfile
        (parsedWalk: ParsedWalk<'TPolicyRecord, 'TStepResults>) =

            let openingDataStageSteps =
                parsedWalk.OpeningDataStage.WithinStageSteps

            let postOpeningDataStageSteps =
                parsedWalk.PostOpeningDataStages
                |> List.map _.WithinStageSteps

            let parsedStepsByDataStage =
                openingDataStageSteps :: postOpeningDataStageSteps
        
            fun dataChangeProfile ->
                let dataChangeGroupIdxs =
                    dataChangeProfile
                    |> List.unfold (fun rem ->
                        if rem > 0 then Some (rem &&& 1, rem / 2) else None)
                    |> List.scan (fun acc x -> acc + x) 0


                (dataChangeGroupIdxs)



                let parsedStepTuplesByGroup =
                    (dataChangeGroupIdxs, parsedWalk.ParsedSteps)
                    ||> List.zip
                    |> List.groupBy fst
                    // Just in case the grouping doesn't preserver ordering.
                    |> List.sortBy fst
                    |> List.map (snd >> List.map snd)
                    
                let dataStageEvaluators =
                    parsedStepTuplesByGroup
                    |> List.map (fun tuples ->
                        let dataStageHdr, _ =
                            List.head tuples

                        let dataStageUid =
                            StepUid dataStageHdr.Uid

                        let parsedSources =
                            tuples
                            |> List.map snd

                        createDataStageEvaluator parsedSources)

                dataStageEvaluators


    let private createRemainingPolicyEvaluator<'TPolicyRecord, 'TStepResults, 'TApiCollection>
        (parsedWalk: ParsedWalk<'TPolicyRecord, 'TStepResults>) =
            (*
            Design Decision:
                For a given record, we could process one data stage at a time.
                However, if a data stage is going to fail, it is better we find
                this out as soon as possible so we don't waste time making
                API calls that will just be discarded anyway!
            *)

            let cachedDataStageEvaluators =
                // This must be concurrent given we'll have multiple records in flight
                // at the same time.
                new ConcurrentDictionary<int, 'TPolicyRecord -> _> ()
            
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

                    // Note that this will EXCLUDE the opening policy record itself.
                    // If, for example, this was a list of None's, that'd suggest that
                    // the opening record and closing records are the same.
                    let! postOpeningDataChanges =
                        extractDataChanges openingPolicyRecord parsedWalk.PostOpeningDataStages

                    assert (postOpeningDataChanges.Length = parsedWalk.PostOpeningDataStages.Length)

                    // Represents a hash of the steps where data has changed.
                    let dataChangeProfile =
                        postOpeningDataChanges
                        |> Seq.indexed
                        |> Seq.sumBy (function | idx, Some _ -> 1 <<< idx | _ -> 0)
                       
                    

                    let policyDataByGroup =
                        policyRecordDataChanges
                        // Convert our list of maybe records into a list of, well, records.
                        |> List.choose id
                        // Bring in the opening policy record.
                        |> List.append [ openingPolicyRecord ]


                    return 0
                }
    *)
                


    let private createSingleStepEvaluator<'TPolicyRecord, 'TStepResults, 'TApiCollection>
        (dataSource, parsedSource: ParsedSource<'TPolicyRecord, 'TStepResults>) =
            let evaluator =
                createDataStageEvaluator
                    [ parsedSource ]

            fun policyRecord ->
                backgroundTask {
                    let evaluationStart =
                        DateTime.Now

                    let! stepResult, apiTelemetry =
                        evaluator policyRecord

                    let evaluationEnd =
                        DateTime.Now

                    let stepResult' =
                        stepResult |> Result.map List.head

                    let evaluationApiTelemetry =
                        apiTelemetry 
                        |> List.map (fun telemetry ->
                            {
                                DataSource      = dataSource
                                EndpointId      = telemetry.EndpointId
                                ProcessingStart = telemetry.ProcessingStart
                                ProcessingEnd   = telemetry.ProcessingEnd                            
                            })

                    let evaluationTelemetry =
                        {
                            EvaluationStart = evaluationStart
                            EvaluationEnd = evaluationEnd
                            ApiRequestTelemetry = evaluationApiTelemetry
                        }

                    return stepResult', evaluationTelemetry
                }


    let private createExitedPolicyEvaluator<'TPolicyRecord, 'TStepResults, 'TApiCollection>
        (parsedWalk: ParsedWalk<'TPolicyRecord, 'TStepResults>) =
            let exitedStepHdr, exitedParsedStep =
                // This CANNOT fail! If it does, I've not idea
                // how the logic managed to get this far!
                List.head parsedWalk.ParsedSteps

            assert checkStepType<OpeningReRunStep<'TPolicyRecord, 'TStepResults, 'TApiCollection>> exitedStepHdr

            let evaluator =
                createSingleStepEvaluator (TelemetryDataSource.OpeningData, exitedParsedStep)

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
                createSingleStepEvaluator (TelemetryDataSource.ClosingData, newRecordsParsedStep)

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






                
                



namespace AnalysisOfChangeEngine.Controller


[<RequireQualifiedAccess>]
module Evaluator =

    open System
    open System.Collections.Concurrent
    open System.Collections.Generic
    open System.Threading.Tasks
    open FsToolkit.ErrorHandling
    open AnalysisOfChangeEngine
    open AnalysisOfChangeEngine.Common
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


    // This creates an executor for a list of steps, in the order provided.
    // It makes no assumption about whether these belong to the same data-stage or not.
    // We use the term executor as it is not concerned with returning (for example)
    // evaluation telemetry.
    let private createExecutorForSteps
        (parsedSources: ParsedSource<'TPolicyRecord, 'TStepResults> list) =
            let apiDependencies =
                parsedSources
                |> List.map _.ApiCalls

            // Convert our list of lists into a single set.
            let combinedApiDependencies =
                apiDependencies
                |> Set.unionMany

            // Now we can group API requests by the underlying requestor.
            let groupedDependenciesByRequestor =
                combinedApiDependencies
                |> Seq.groupBy _.Requestor
                |> Seq.map (fun (requestor, dependencies) ->
                    requestor, Seq.toArray dependencies)
                |> Map.ofSeq

            // Call each unique API requestor asking for as many outputs
            // as required. Basically, we're trying to get as much output
            // from each and every API call.
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
                // Pre-compute as much in-advance before we need to do
                // something with the supplied policy record.
                deferredApiCallTasksByRequestor
                |> Map.map (fun _ asyncExecutor -> asyncExecutor policyRecord)

            let responseArrayHydrators =
                apiDependencies
                |> List.map (fun stepDependencies ->
                    // For each dependency in our current step, create a function
                    // let will take the API response and return the corresponding output.
                    let dependencyGetter =
                        stepDependencies
                        |> Seq.map (fun dependency ->
                            // We deriving this up-front as won't change.
                            let nestedIdxWithinRequestor =
                                // Find the specific API requestor from our grouped
                                // collection of API requestors.
                                groupedDependenciesByRequestor[dependency.Requestor]
                                // Once we have that, find where our specific output
                                // is in the returned array.
                                |> Array.findIndex _.Equals(dependency)

                            fun (apiResponses: Map<_, obj array>) ->
                                let responsesWithinRequestor =
                                    apiResponses[dependency.Requestor]

                                responsesWithinRequestor[nestedIdxWithinRequestor])
                        |> Seq.toArray
                        
                    fun apiResponses idx ->
                        dependencyGetter[idx] apiResponses)

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
                                            // Hydrator requires our grouped API responses.
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

    // This is used for the exited and new records only. This is because such
    // policies only require a single step to be run.
    let private createExecutorForStep
        (parsedSource: ParsedSource<'TPolicyRecord, 'TStepResults>) =
            // Running a single step is the same as running a list of just one step!
            let executor =
                createExecutorForSteps
                    [ parsedSource ]

            fun policyRecord ->
                backgroundTask {
                    let! stepResult, apiTelemetry =
                        executor policyRecord

                    let stepResult' =
                        // We're only expecting results for a single step to be returned.
                        stepResult
                        |> Result.map List.exactlyOne

                    return stepResult', apiTelemetry
                }


    let private createEvaluatorsForProfile
        (parsedWalk: ParsedWalk<'TPolicyRecord, 'TStepResults>) =
            // The slight complexity here is that we're trying to determine
            // the data source we want to use for telemetry purposes.
            // TODO - Arguably... This could be done as part of the parsing process?
            let allDataStages =
                parsedWalk.PostOpeningDataStages
                |> List.map _.WithinStageSteps
                |> List.map (fun parsedSteps ->
                    let dataSource =
                        match parsedSteps.Head with
                        | :? MoveToClosingDataStep<'TPolicyRecord, 'TStepResults>, _ ->
                            TelemetryDataSource.ClosingData
                        | hdr, _ ->
                            TelemetryDataSource.DataChangeStep hdr.Uid

                    parsedSteps, dataSource)

            // In theory, this (parent) evaluator function will only ever be called
            // from synchronized code. As such, no need to support concurrent writes.
            let cachedGroupEvaluators =
                new Dictionary<Guid list, _> ()
        
            fun (dataStageGroupingIdxs: int list) ->
                assert (dataStageGroupingIdxs.Length = allDataStages.Length)

                let groupedParsedSteps =
                    (dataStageGroupingIdxs, allDataStages)
                    ||> List.zip 
                    |> List.groupBy fst
                    |> List.map (fun (_, steps) ->
                        let steps' =
                            steps
                            |> List.map snd

                        // We use the data source from the first data stage in our group.
                        let dataSource =
                            steps'
                            |> List.head
                            |> snd

                        let collectedSteps =
                            steps'
                            |> List.collect fst
                            
                        collectedSteps, dataSource)

                assert (dataStageGroupingIdxs.Length = groupedParsedSteps.Length)

                let groupedUids =
                    groupedParsedSteps
                    |> List.map (fst >> List.map (fst >> _.Uid))

                let groupedParsedSources =
                    groupedParsedSteps
                    |> List.map (fun (parsedSteps, dataSource) ->
                        let parsedSources =
                            parsedSteps |> List.map snd

                        parsedSources, dataSource)

                let groupedExecutors =
                    (groupedUids, groupedParsedSources)
                    ||> List.map2 (fun stepUids (parsedSources, dataSource) ->
                        cachedGroupEvaluators.GetOrAdd (stepUids, fun _ ->
                            let executor =
                                // We need to 
                                createExecutorForSteps parsedSources

                            fun policyRecord ->
                                backgroundTask {
                                    let! groupOutcome, apiTelemetry =
                                        executor policyRecord

                                    let evaluationApiTelemetry =
                                        apiTelemetry
                                        |> List.map (fun telemetry ->
                                            {
                                                DataSource      = dataSource
                                                EndpointId      = telemetry.EndpointId
                                                ProcessingStart = telemetry.ProcessingStart
                                                ProcessingEnd   = telemetry.ProcessingEnd
                                            })

                                    return groupOutcome, evaluationApiTelemetry
                                }))
                    
                groupedExecutors

    let private extractDataChanges (openingPolicyRecord, closingPolicyRecord) = 
        let rec inner priorPolicyRecord
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
                        (inner priorPolicyRecord dss)
                | Ok (Some newRecord as newRecord') ->
                    Result.bind
                        (fun subsequentStages -> Ok (newRecord'::subsequentStages))
                        // However, if the data change step _does_ lead to a new record,
                        // then that will need to be carried forward instead.
                        (inner newRecord dss)
                | Error reason ->
                    // If we encounter an error... Then we can immediately return.
                    Error (EvaluationFailure.DataChangeFailure
                        (ds.DataChangeStep, [| reason |]))
            | [] ->
                Ok [] 
                
        inner openingPolicyRecord
            
    type private DataStageExecutor<'TPolicyRecord, 'TStepResults> =
        'TPolicyRecord -> Task<Result<'TStepResults list, EvaluationFailure list> * (EvaluationApiRequestTelemetry list)>

    let rec private executeDataStageGroups<'TPolicyRecord, 'TStepResults>
        (groupedDataAndExecutors: ('TPolicyRecord * DataStageExecutor<'TPolicyRecord, 'TStepResults>) list) =
            match groupedDataAndExecutors with
                | (policyData, executor)::xs ->
                    backgroundTask {
                        let! dataStageOutcome, telemetry =
                            executor policyData

                        let! allGroupsOutcomes =
                            match dataStageOutcome with
                            | Ok dataStageResults ->
                                backgroundTask {
                                    let! nextOutcomes =
                                        executeDataStageGroups xs

                                    return
                                        match nextOutcomes with
                                        | Ok nextDataStageResults, nextTelemetry ->
                                            Ok (dataStageResults::nextDataStageResults), telemetry::nextTelemetry
                                        | Error nextFailures, nextTelemetry ->
                                            Error nextFailures, telemetry::nextTelemetry
                                }

                            | Error dataStageFailures ->
                                backgroundTask {
                                    return Error dataStageFailures, [telemetry]
                                }

                        return allGroupsOutcomes                            
                    }
                | [] ->
                    backgroundTask {
                        return Ok [[]], [[]]
                    }

    // We intentionally refer to this as an evaluator, particularly given it's returning
    // telemetry for the evaluation itself.
    let private createRemainingPolicyEvaluator<'TPolicyRecord, 'TStepResults when 'TPolicyRecord: equality>
        (parsedWalk: ParsedWalk<'TPolicyRecord, 'TStepResults>) =
            (*
            Design Decision:
                For a given record, we could process all data stages in one go.
                However, if a data stage is going to fail, it is better we find
                this out as soon as possible so we don't waste time making
                API calls that will just be discarded anyway.
            *)

            let cachedGroupedEvaluators =
                // This MUST be concurrent given we'll have multiple records in flight
                // at the same time.
                new ConcurrentDictionary<int list, _ list> ()

            let groupedEvaluatorsFactory =
                createEvaluatorsForProfile parsedWalk
            
            fun (RemainingPolicy (openingPolicyRecord, closingPolicyRecord)) ->   
                let evaluationStart =
                    DateTime.Now

                // Note that this will EXCLUDE the opening policy record itself.
                // If, for example, this was a list of None's, that'd suggest that
                // the opening record and closing records are the same.
                let postOpeningDataChanges =
                    extractDataChanges
                        (openingPolicyRecord, closingPolicyRecord)
                        parsedWalk.PostOpeningDataStages
                            
                match postOpeningDataChanges with
                | Ok postOpeningDataChanges' ->
                    backgroundTask {
                        assert (postOpeningDataChanges'.Length = parsedWalk.PostOpeningDataStages.Length)
                       
                        let dataStageGroupingIdxs =
                            postOpeningDataChanges'
                            |> List.scan (fun priorIdx -> function
                                | Some _    -> priorIdx + 1
                                | None      -> priorIdx) 0
                            // Don't drop the initial state as that corresponds to the opening data stage.

                        let policyDataByGroup =
                            postOpeningDataChanges'
                            // Convert our list of maybe records into a list of, well, records.
                            |> List.choose id
                            // Bring in the opening policy record.
                            |> List.append [ openingPolicyRecord ]

                        assert (policyDataByGroup.Length = dataStageGroupingIdxs.Length)                       

                        let evaluatorsByGroup =
                            // Make sure we re-use any cached evaluators for this given profile.
                            cachedGroupedEvaluators.GetOrAdd
                                (dataStageGroupingIdxs, groupedEvaluatorsFactory)

                        assert (evaluatorsByGroup.Length = dataStageGroupingIdxs.Length)

                        let! allStepResults, evaluationApiTelemetry =
                            (policyDataByGroup, evaluatorsByGroup)
                            ||> List.zip
                            |> executeDataStageGroups                        

                        let allStepResults' =
                            allStepResults
                            |> Result.map List.concat

                        let evaluationApiTelemetry' =
                            List.concat evaluationApiTelemetry

                        let evaluationEnd =
                            DateTime.Now

                        let evaluationTelemetry =
                            {
                                EvaluationStart     = evaluationStart
                                EvaluationEnd       = evaluationEnd
                                ApiRequestTelemetry = evaluationApiTelemetry'
                            }

                        return allStepResults', evaluationTelemetry
                    }

                | Error reason ->
                    backgroundTask {
                        let evaluationTelemetry =
                            {
                                EvaluationStart     = evaluationStart
                                EvaluationEnd       = DateTime.Now
                                ApiRequestTelemetry = []
                            }

                        return Error [ reason ], evaluationTelemetry
                    }
                    
    let private createEvaluatorForStep (dataSource, parsedSource) =
        let executor =
            createExecutorForStep parsedSource

        fun policyRecord ->
            backgroundTask {
                let evaluationStart =
                    DateTime.Now

                let! outcome, apiTelemetry =
                    executor policyRecord

                let evaluationEnd =
                    DateTime.Now

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
                        EvaluationStart     = evaluationStart
                        EvaluationEnd       = evaluationEnd
                        ApiRequestTelemetry = evaluationApiTelemetry
                    }

                return outcome, evaluationTelemetry
            }                        

    // Arguably, we're only asking for the API collection type for the purpose of type assertion.
    let private createExitedPolicyEvaluator<'TPolicyRecord, 'TStepResults, 'TApiCollection>
        (parsedWalk: ParsedWalk<'TPolicyRecord, 'TStepResults>) =
            let exitedStepHdr, exitedParsedStep =
                // This CANNOT fail! If it does, I've not idea
                // how the logic managed to get this far!
                List.head parsedWalk.ParsedSteps

            // Let's just make sure we're dealing with the step we're actually expecting!
            assert checkStepType<OpeningReRunStep<'TPolicyRecord, 'TStepResults, 'TApiCollection>> exitedStepHdr

            let evaluator =
                createEvaluatorForStep (TelemetryDataSource.OpeningData, exitedParsedStep)

            fun (ExitedPolicy policyRecord) ->
                evaluator policyRecord


    // Note the comments for the exited policy evaluator above.
    let private createNewPolicyEvaluator<'TPolicyRecord, 'TStepResults, 'TApiCollection>
        (parsedWalk: ParsedWalk<'TPolicyRecord, 'TStepResults>) =
            let newRecordsStepHdr, newRecordsParsedStep =
                List.last parsedWalk.ParsedSteps

            assert checkStepType<AddNewRecordsStep<'TPolicyRecord, 'TStepResults>> newRecordsStepHdr

            let evaluator =
                createEvaluatorForStep (TelemetryDataSource.ClosingData, newRecordsParsedStep)

            fun (NewPolicy policyRecord) ->
                evaluator policyRecord                


    let create (walk: AbstractWalk<'TPolicyRecord, 'TStepResults, 'TApiCollection>) apiCollection =
        let parsedWalk =
            WalkParser.execute walk apiCollection

        let exitedRecordEvaluator =
            // If we don't specificy the API collection type, obj will be inferred
            // which will lead to chaos via a type assertion failure at runtime.
            createExitedPolicyEvaluator<_, _, 'TApiCollection> parsedWalk

        let remainingRecordEvaluator =
            createRemainingPolicyEvaluator parsedWalk

        let newRecordEvaluator =
            createNewPolicyEvaluator<_, _, 'TApiCollection> parsedWalk

        {
            new IPolicyEvaluator<'TPolicyRecord, 'TStepResults> with

                member _.Execute (ExitedPolicy _ as exitedPolicyRecord) =
                    exitedRecordEvaluator exitedPolicyRecord

                member _.Execute (RemainingPolicy _ as remainingPolicy) =
                    remainingRecordEvaluator remainingPolicy

                member _.Execute (NewPolicy _ as newPolicyRecord) =
                    newRecordEvaluator newPolicyRecord
        }               

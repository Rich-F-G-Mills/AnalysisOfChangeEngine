
namespace AnalysisOfChangeEngine.Controller


[<RequireQualifiedAccess>]
module Evaluator =

    open System
    open System.Collections.Concurrent
    open System.Threading.Tasks
    open FsToolkit.ErrorHandling
    open AnalysisOfChangeEngine
    open AnalysisOfChangeEngine.Common
    open AnalysisOfChangeEngine.Controller.WalkAnalyser


    [<NoEquality; NoComparison>]
    /// Provides timing information for a given API request. This will always be provided,
    /// irrespective of whether the request was successful or not.
    type private TaggedApiRequestTelemetry =
        {
            RequestorName   : string
            EndpointId      : string option
            Submitted       : DateTime
            ProcessingStart : DateTime
            ProcessingEnd   : DateTime
        }


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
            let apiDependenciesByStep =
                parsedSources
                |> List.map _.ApiCalls

            // Convert our list of lists into a single set.
            let combinedApiDependencies =
                apiDependenciesByStep
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

                    fun policyRecord ->
                        backgroundTask {
                            let submitted =
                                DateTime.Now

                            let! apiResponse, apiTelemetry =
                                asyncExecutor policyRecord

                            let taggedTelemetry =
                                apiTelemetry
                                |> Option.map (fun telemetry ->
                                    {
                                        RequestorName   = requestor.Name
                                        EndpointId      = telemetry.EndpointId
                                        // Note that all API requests for these steps were
                                        // submitted at the same time.
                                        Submitted       = submitted
                                        ProcessingStart = telemetry.ProcessingStart
                                        ProcessingEnd   = telemetry.ProcessingEnd
                                    })

                            return apiResponse, taggedTelemetry
                        })

            // Execute all API calls within a given group.
            let groupedDependencyExecutor policyRecord =
                // Pre-compute as much in-advance before we need to do
                // something with the supplied policy record.
                deferredApiCallTasksByRequestor
                |> Map.map (fun _ asyncExecutor -> asyncExecutor policyRecord)

            let responseArrayHydrators =
                apiDependenciesByStep
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

                            assert (stepResults.Length = parsedSources.Length)

                            Ok stepResults

                        | Error failures ->
                            // Necessary as we're mapping from one result domain to another.
                            Error failures

                    let apiRequestsTelemetry =
                        responsesByRequestor
                        |> Map.values
                        // Drop any missing telemetries.
                        |> Seq.choose snd
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
                    
    // We're an evaluator now we're returning evaluation telemetry.
    let private createEvaluatorForStep (dataSource, parsedSource) =
        let executor =
            createExecutorForStep parsedSource

        fun policyRecord ->
            backgroundTask {
                let evaluationStart =
                    DateTime.Now

                let! outcome, taggedTelemetry =
                    executor policyRecord

                let evaluationEnd =
                    DateTime.Now

                let evaluationApiTelemetry =
                    taggedTelemetry
                    |> List.map (fun telemetry ->
                        {
                            RequestorName   = telemetry.RequestorName
                            DataSource      = dataSource
                            EndpointId      = telemetry.EndpointId
                            Submitted       = telemetry.Submitted
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


    (*
    --- REMAINING POLICY RECORD SPECIFIC LOGIC ---
    The following is logic used to run policy records present at both the opening and closing steps.
    The logic itself is very complicated... So you better deal with it!
    Reason being that we have a number of aspects to deal with:
        * The need to combine data stages together where a data change step does NOT lead to a change
            in policy data.
        * The need to validate steps, which requires the threading of outputs and data from prior steps/stages.
        * The fact that evaluation can fail for a number of reasons that must be captured and returned.
        * The need to aggregate API telemetry.
        * ...and cope with the fact that we're running this asyncronously.
    *)

    let private extractPostOpeningDataChanges (openingPolicyRecord, closingPolicyRecord) = 
        let rec inner priorPolicyRecord
            : IDataChangeStep<'TPolicyRecord> list -> _ = function
            | dcs::dcss ->
                let newRecordForDataStage =
                    dcs.DataChanger (openingPolicyRecord, priorPolicyRecord, closingPolicyRecord)
                                        
                match newRecordForDataStage with
                | Ok None ->
                    Result.bind
                        (fun subsequentStages -> Ok (None::subsequentStages))
                        // If the current data change step hasn't changed anything,
                        // then we'll just carry forward our inherited view of the
                        // latest policy record.
                        (inner priorPolicyRecord dcss)
                | Ok (Some newRecord as newRecord') ->
                    Result.bind
                        (fun subsequentStages -> Ok (newRecord'::subsequentStages))
                        // However, if the data change step _does_ lead to a new record,
                        // then that will need to be carried forward instead.
                        (inner newRecord dcss)
                | Error reason ->
                    // If we encounter an error... Then we can immediately return.
                    Error (EvaluationFailure.DataChangeFailure
                        (dcs, [| reason |]))
            | [] ->
                Ok [] 
                
        inner openingPolicyRecord


    [<NoEquality; NoComparison>]
    type private GroupingsByDataStage<'TPolicyRecord, 'TStepResults> =
        {
            ParsedSources           : ParsedSource<'TPolicyRecord, 'TStepResults> list list
            StepHeaders             : IStepHeader list list
            StepUids                : Guid list list
            TelemetryDataSources    : TelemetryDataSource list
        }

    [<NoEquality; NoComparison>]
    type private GroupingsByProfile<'TPolicyRecord, 'TStepResults> =
        {       
            ParsedSources           : ParsedSource<'TPolicyRecord, 'TStepResults> list list
            StepHeaders             : IStepHeader list list
            StepUids                : Guid list list
            TelemetryDataSources    : TelemetryDataSource list
        }

    let private getGroupingsByDataStage<'TPolicyRecord, 'TStepResults, 'TApiCollection when 'TPolicyRecord: equality>
        (parsedWalk: ParsedWalk<'TPolicyRecord, 'TStepResults>) 
        : GroupingsByDataStage<_, _> =
            let parsedStepsByDataStage =
                parsedWalk.PostOpeningDataStages
                |> List.map _.WithinStageSteps
                |> List.append [ parsedWalk.OpeningDataStage.WithinStageSteps ]

            let stepHeadersByDataStage =
                parsedStepsByDataStage
                |> List.innerMap fst

            let stepUidsByDataStage =
                stepHeadersByDataStage
                |> List.innerMap _.Uid

            let parsedSourcesByDataStage =
                parsedStepsByDataStage
                |> List.innerMap snd

            let telemetryDataSourcesByDataStage =
                stepHeadersByDataStage
                |> List.map List.head
                |> List.map (function
                    | :? OpeningReRunStep<'TPolicyRecord, 'TStepResults, 'TApiCollection> ->
                        TelemetryDataSource.OpeningData
                    | :? MoveToClosingDataStep<'TPolicyRecord, 'TStepResults> ->
                        TelemetryDataSource.ClosingData
                    | :? DataChangeStep<'TPolicyRecord, 'TStepResults> as hdr ->
                        TelemetryDataSource.DataChangeStep hdr.Uid
                    | _ ->
                        failwith "Unexpected data change step type.")

            {
                ParsedSources           = parsedSourcesByDataStage
                StepHeaders             = stepHeadersByDataStage
                StepUids                = stepUidsByDataStage
                TelemetryDataSources    = telemetryDataSourcesByDataStage
            }

    let private getGroupingsByProfile (groupingsByDataStage: GroupingsByDataStage<_, _>) (profile: int list)
        : GroupingsByProfile<_, _> =
            let reGroupByProfile (xs: _ list) =
                assert (xs.Length = profile.Length)

                xs
                |> List.zip profile
                |> List.groupBy fst
                |> List.map (snd >> List.collect snd)

            {
                ParsedSources           = reGroupByProfile groupingsByDataStage.ParsedSources
                StepHeaders             = reGroupByProfile groupingsByDataStage.StepHeaders
                StepUids                = reGroupByProfile groupingsByDataStage.StepUids
                TelemetryDataSources    =
                    groupingsByDataStage.TelemetryDataSources
                    |> List.map List.singleton
                    |> reGroupByProfile
                    // However, we only care about the first telemetry data source
                    // without our profile groupings.
                    |> List.map List.head
            }

    type private DataStageExecutor<'TPolicyRecord, 'TStepResults> =
        'TPolicyRecord -> Task<Result<'TStepResults list, EvaluationFailure list> * (TaggedApiRequestTelemetry list)>

    (*
    Design Decision:
        We have the a complication in that the opening re-run step can be run without any prior step results.
        Arguably, we could have separate execution and validation logic for the opening re-run step.
        However, this would lead to extra complexity for seemingly dogmatic reasons.    
    *)
    let rec private tryFindValidationFailureWithinProfileStage<'TPolicyRecord, 'TStepResults, 'TApiCollection when 'TPolicyRecord: equality>
        (policyRecordForPriorStage, policyRecordForStage, priorStepResults: 'TStepResults option)
        : (IStepHeader * 'TStepResults) list -> _ = function
        | (hdr, currentStepResults)::xs ->
            let stepValidationOutcome =
                match priorStepResults, hdr with
                // Only the opening re-run step can have no prior results.
                | _, (:? OpeningReRunStep<'TPolicyRecord, 'TStepResults, 'TApiCollection> as hdr) ->
                    hdr.Validator (policyRecordForStage, priorStepResults, currentStepResults)
                | Some priorStepResults', (:? SourceChangeStep<'TPolicyRecord, 'TStepResults, 'TApiCollection> as hdr) ->
                    hdr.Validator (policyRecordForStage, priorStepResults', currentStepResults)
                | Some priorStepResults', (:? DataChangeStep<'TPolicyRecord, 'TStepResults> as hdr) ->
                    hdr.Validator (policyRecordForPriorStage, priorStepResults', policyRecordForStage, currentStepResults)
                | Some priorStepResults', (:? MoveToClosingDataStep<'TPolicyRecord, 'TStepResults> as hdr) ->
                    hdr.Validator (policyRecordForPriorStage, priorStepResults', policyRecordForStage, currentStepResults)
                | Some _, (:? AddNewRecordsStep<'TPolicyRecord, 'TStepResults> as hdr) ->
                    hdr.Validator (policyRecordForPriorStage, currentStepResults)
                | Some _, :? RemoveExitedRecordsStep<'TPolicyRecord, 'TStepResults> ->
                    StepValidationOutcome.Empty
                | _ ->
                    failwith "Unexpected error."

            match stepValidationOutcome, xs with
            | StepValidationOutcome.Completed [||], [] ->
                // No issues and nothing else left to validate within this stage.
                None
            | StepValidationOutcome.Completed [||], xs ->
                // No issues, so carry on.
                tryFindValidationFailureWithinProfileStage<_, _, 'TApiCollection>
                    (policyRecordForPriorStage, policyRecordForStage, Some currentStepResults) xs
            | StepValidationOutcome.Completed issues, _ ->
                // We have some issues, so return them.
                Some (EvaluationFailure.ValidationFailure (hdr, issues))
            | StepValidationOutcome.Aborted reason, _ ->
                // We have an aborted validation, so return it.
                Some (EvaluationFailure.ValidationAborted (hdr, [| reason |]))

        | [] ->
            None

    // Note that priorStepResults is for the prior step, NOT the prior stage. Not to say
    // those results don't come from the final run within the prior stage!
    let rec private executeProfile<'TPolicyRecord, 'TStepResults, 'TApiCollection when 'TPolicyRecord: equality>
        (policyRecordForPriorStage: 'TPolicyRecord, priorStepResults: 'TStepResults option) = function
        | (policyRecordForStage: 'TPolicyRecord, (stepHdrsForStage, stageExecutor: DataStageExecutor<_, _>, dataSourceForStage))::xs ->
            backgroundTask {
                let! stageResults, taggedTelemetry =
                    stageExecutor policyRecordForStage

                // Regardless of the outcome above, we need to construct our API
                // request telemetry.
                let evaluationApiTelemtry =
                    taggedTelemetry
                    |> List.map (fun (telemetry: TaggedApiRequestTelemetry) ->
                        {
                            RequestorName   = telemetry.RequestorName
                            DataSource      = dataSourceForStage
                            EndpointId      = telemetry.EndpointId
                            Submitted       = telemetry.Submitted
                            ProcessingStart = telemetry.ProcessingStart
                            ProcessingEnd   = telemetry.ProcessingEnd
                        })

                let! profileOutcomes =
                    match stageResults with
                    | Ok (stageResults': 'TStepResults list) ->
                        let validationIssues =
                            (stepHdrsForStage, stageResults')
                            ||> List.zip
                            |> tryFindValidationFailureWithinProfileStage<_, _, 'TApiCollection>
                                (policyRecordForPriorStage, policyRecordForStage, priorStepResults)

                        match validationIssues with
                        | Some failure ->
                            backgroundTask {
                                // We have a validation failure, so return it.
                                return Error [ failure ], [ evaluationApiTelemtry ]
                            }                        

                        | None ->
                            backgroundTask {
                                let finalStepResultsForGroup =
                                    List.last stageResults'

                                let! remOutcomes, remEvaluationApiTelemtry =
                                    // Feed in the policy data for this group into the next as prior policy data.
                                    // Furthermore, we also need to carry forward the results of the final step
                                    // for this group.
                                    executeProfile<_, _, 'TApiCollection>
                                        (policyRecordForStage, Some finalStepResultsForGroup) xs

                                return
                                    match remOutcomes with
                                    | Ok remStageResults ->
                                        Ok (stageResults'::remStageResults), evaluationApiTelemtry::remEvaluationApiTelemtry
                                    | Error nextFailures ->
                                        Error nextFailures, evaluationApiTelemtry::remEvaluationApiTelemtry
                            }

                    | Error dataStageFailures ->
                        backgroundTask {
                            return Error dataStageFailures, [ evaluationApiTelemtry ]
                        }

                return profileOutcomes                            
            }

        | [] ->
            backgroundTask {
                return Ok [[]], [[]]
            }


    type private ProfileExecutor<'TPolicyRecord, 'TStepResults when 'TPolicyRecord: equality> =
        'TPolicyRecord * 'TStepResults option * 'TPolicyRecord list -> 
            Task<Result<'TStepResults list list, EvaluationFailure list> * (EvaluationApiRequestTelemetry list list)>


    let private createRemainingPolicyEvaluator<'TPolicyRecord, 'TStepResults, 'TApiCollection when 'TPolicyRecord: equality>
        (logger: ILogger)
        (parsedWalk: ParsedWalk<'TPolicyRecord, 'TStepResults>) =

            let groupingsByDataStage =
                getGroupingsByDataStage<_, _, 'TApiCollection> parsedWalk

            let postOpeningDataChangeSteps =
                parsedWalk.PostOpeningDataStages
                |> List.map _.DataChangeStep

            // Note that value factories for a concurrent CAN be run in parallel. However,
            // only the first to complete will be retained.
            // As such, where we have value factory logic referring to a dictionary,
            // that dictionary MUST also be concurrent!
            let cachedGroupingsForProfile =
                new ConcurrentDictionary<int list, GroupingsByProfile<_, _>> ()

            let cachedExecutorsForUids =
                new ConcurrentDictionary<Guid list, DataStageExecutor<_, _>> ()

            let cachedProfileExecutors =
                new ConcurrentDictionary<int list, ProfileExecutor<_, _>> ()


            fun (RemainingPolicy (openingPolicyRecord, closingPolicyRecord), priorClosingStepResults) ->   
                let evaluationStart =
                    DateTime.Now

                let postOpeningDataChanges =
                    extractPostOpeningDataChanges (openingPolicyRecord, closingPolicyRecord) postOpeningDataChangeSteps

                match postOpeningDataChanges with
                | Ok postOpeningDataChanges' ->
                    assert (postOpeningDataChanges'.Length = parsedWalk.PostOpeningDataStages.Length)
                       
                    // Here we create a list of indexes which indicate the grouping of data stages.
                    let groupingProfileIdxs =
                        postOpeningDataChanges'
                        |> List.scan (fun priorIdx -> function
                            | Some _    -> priorIdx + 1
                            | None      -> priorIdx) 0
                        // Don't drop the initial state as this corresponds to the opening data stage.

                    let policyRecordsForProfile =
                        postOpeningDataChanges'
                        |> List.choose id
                        |> List.append [ openingPolicyRecord ]

                    let profileExecutor =
                        cachedProfileExecutors.GetOrAdd(groupingProfileIdxs, fun _ ->
                            let groupingsForProfile =
                                cachedGroupingsForProfile.GetOrAdd(groupingProfileIdxs, fun _ ->
                                    do logger.LogDebug (sprintf "Creating groupings by profile for %A." groupingProfileIdxs)

                                    getGroupingsByProfile groupingsByDataStage groupingProfileIdxs)

                            let executorsByProfileStage =
                                (groupingsForProfile.StepUids, groupingsForProfile.ParsedSources)
                                ||> List.map2 (fun uids sources ->
                                        do logger.LogDebug
                                            (sprintf "Creating executor for %ix step UIDs starting with %O."
                                                uids.Length groupingProfileIdxs.Head)

                                        cachedExecutorsForUids.GetOrAdd (uids, fun _ ->
                                            createExecutorForSteps sources))

                            let profileStagesInfo =
                                (groupingsForProfile.StepHeaders, executorsByProfileStage, groupingsForProfile.TelemetryDataSources)
                                |||> List.zip3

                            do logger.LogDebug
                                (sprintf "Created executor for profile %A." groupingProfileIdxs)

                            fun (openingPolicyRecord', priorClosingStepResults', policyRecordsForProfile': _ list) ->
                                assert (policyRecordsForProfile'.Length = profileStagesInfo.Length)

                                // We can't compute this ahead of time as it depends on the
                                // policy records being processed.
                                (policyRecordsForProfile', profileStagesInfo)
                                ||> List.zip
                                |> executeProfile<_, _, 'TApiCollection>
                                    (openingPolicyRecord', priorClosingStepResults')
                        )

                    backgroundTask {
                        let! walkResults, evaluationApiTelemetry =
                            profileExecutor (openingPolicyRecord, priorClosingStepResults, policyRecordsForProfile)   
                            
                        let evaluationApiTelemetry' =
                            List.concat evaluationApiTelemetry                            

                        let evaluationTelemetry =
                            {
                                EvaluationStart     = evaluationStart
                                EvaluationEnd       = DateTime.Now
                                ApiRequestTelemetry = evaluationApiTelemetry'
                            }

                        let walkResults' =
                            walkResults
                            |> Result.map List.concat

                        return walkResults', evaluationTelemetry
                    }

                | Error failure ->
                    let evaluationTelemetry =
                        {
                            EvaluationStart     = evaluationStart
                            EvaluationEnd       = DateTime.Now
                            ApiRequestTelemetry = List.empty
                        }

                    backgroundTask {
                        return Error [ failure ], evaluationTelemetry
                    }

    // ------ END OF REMAINING POLICY RECORD SPECIFIC LOGIC ------
                

    // Arguably, we're only asking for the API collection type for the purpose of type assertion at run-time.
    let private createExitedPolicyEvaluator<'TPolicyRecord, 'TStepResults, 'TApiCollection>
        (parsedWalk: ParsedWalk<'TPolicyRecord, 'TStepResults>) =
            let openingReRunStepHdr, openingReRunParsedStep =
                // This CANNOT fail! If it does, I've not idea
                // how the logic managed to get this far!
                List.head parsedWalk.ParsedSteps

            // Let's just make sure we're dealing with the step we're actually expecting!
            assert checkStepType<OpeningReRunStep<'TPolicyRecord, 'TStepResults, 'TApiCollection>> openingReRunStepHdr

            let openingReRunStep : OpeningReRunStep<'TPolicyRecord, 'TStepResults, 'TApiCollection> =
                downcast openingReRunStepHdr

            let evaluator =
                createEvaluatorForStep (TelemetryDataSource.OpeningData, openingReRunParsedStep)

            fun (ExitedPolicy policyRecord, priorClosingStepResults) ->
                backgroundTask {
                    let! evaluationOutcome, evaluationTelemetry =
                        evaluator policyRecord
                
                    match evaluationOutcome with
                    | Ok stepResults ->
                        let validationOutcome =
                            openingReRunStep.Validator (policyRecord, priorClosingStepResults, stepResults)

                        match validationOutcome with
                        | StepValidationOutcome.Completed [||] ->
                            return evaluationOutcome, evaluationTelemetry

                        | StepValidationOutcome.Completed issues ->
                            return Error [ EvaluationFailure.ValidationFailure
                                (openingReRunStepHdr, issues) ], evaluationTelemetry

                        | StepValidationOutcome.Aborted reason ->
                            return Error [ EvaluationFailure.ValidationAborted
                                (openingReRunStepHdr, [| reason |]) ], evaluationTelemetry

                    | Error _ ->
                        // If we've already errored, there's no point in validating.
                        return evaluationOutcome, evaluationTelemetry
                }

    // Note the comments for the exited policy evaluator above.
    let private createNewPolicyEvaluator<'TPolicyRecord, 'TStepResults, 'TApiCollection>
        (parsedWalk: ParsedWalk<'TPolicyRecord, 'TStepResults>) =
            let newRecordsStepHdr, newRecordsParsedStep =
                List.last parsedWalk.ParsedSteps

            assert checkStepType<AddNewRecordsStep<'TPolicyRecord, 'TStepResults>> newRecordsStepHdr

            let newRecordsStep : AddNewRecordsStep<'TPolicyRecord, 'TStepResults> =
                downcast newRecordsStepHdr

            let evaluator =
                createEvaluatorForStep (TelemetryDataSource.ClosingData, newRecordsParsedStep)

            fun (NewPolicy policyRecord) ->
                backgroundTask {
                    let! evaluationOutcome, evaluationTelemetry =
                        evaluator policyRecord
                
                    match evaluationOutcome with
                    | Ok stepResults ->
                        let validationOutcome =
                            newRecordsStep.Validator (policyRecord, stepResults)

                        match validationOutcome with
                        | StepValidationOutcome.Completed [||] ->
                            return evaluationOutcome, evaluationTelemetry

                        | StepValidationOutcome.Completed issues ->
                            return Error [ EvaluationFailure.ValidationFailure
                                (newRecordsStepHdr, issues) ], evaluationTelemetry

                        | StepValidationOutcome.Aborted reason ->
                            return Error [ EvaluationFailure.ValidationAborted
                                (newRecordsStepHdr, [| reason |]) ], evaluationTelemetry

                    | Error _ ->
                        // If we've already errored, there's no point in validating.
                        return evaluationOutcome, evaluationTelemetry
                }        


    let create
        (logger: ILogger)
        (walk: AbstractWalk<'TPolicyRecord, 'TStepResults, 'TApiCollection>)
        apiCollection =
            let parsedWalk =
                WalkParser.execute walk apiCollection

            let exitedRecordEvaluator =
                // If we don't specificy the API collection type, obj will be inferred
                // which will lead to chaos via a type assertion failure at runtime.
                createExitedPolicyEvaluator<_, _, 'TApiCollection> parsedWalk

            let remainingRecordEvaluator =
                createRemainingPolicyEvaluator<_, _, 'TApiCollection> logger parsedWalk

            let newRecordEvaluator =
                createNewPolicyEvaluator<_, _, 'TApiCollection> parsedWalk

            {
                new IPolicyEvaluator<'TPolicyRecord, 'TStepResults> with

                    member _.Execute (ExitedPolicy _ as exitedPolicyRecord, priorClosingStepResults) =
                        exitedRecordEvaluator (exitedPolicyRecord, priorClosingStepResults)

                    member _.Execute (RemainingPolicy _ as remainingPolicy, priorClosingStepResults) =
                        remainingRecordEvaluator (remainingPolicy, priorClosingStepResults)

                    member _.Execute (NewPolicy _ as newPolicyRecord) =
                        newRecordEvaluator newPolicyRecord
            }               

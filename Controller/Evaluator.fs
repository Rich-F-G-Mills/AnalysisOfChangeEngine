
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


    // TODO - We're frequently having to pass around a generic type for the
    // API collection. Ultimately, this is only needed for type assertions and
    // type-based pattern matching. Could be avoided by creating a step-specific
    // interface for the opening re-run and source change steps?


    // We need to move from the world of API failures into the world of
    // failed evaluations.
    let private failureMapper (requestor: AbstractApiRequestor<_>) = function
        | ApiRequestFailure.CalculationFailure reasons ->
            WalkEvaluationFailure.ApiCalculationFailure (requestor.Name, reasons)
        | ApiRequestFailure.CallFailure reasons ->
            WalkEvaluationFailure.ApiCallFailure (requestor.Name, reasons)


    // We need to at least give a type hint for the abstract requestor, otherwise
    // we cannot access the 'Name' property.
    let private apiResponsesConsolidator
        (acc: Result<Map<AbstractApiRequestor<_>, obj array>, _>)
        (apiRequstor, apiResponse) =
            match acc, apiRequstor, apiResponse with
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
    // Other than to ensure that step results have been successfully constructed,
    // it doesn't perform any other validation.
    let private createEvaluatorForSteps
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

                    let requestorName =
                        RequestorName requestor.Name

                    let asyncExecutor =
                        requestor.ExecuteAsync outputs

                    fun (policyRecord, onApiRequestProcessingStart) ->
                        asyncExecutor (policyRecord, onApiRequestProcessingStart requestorName))

            // Execute all API calls within a given group.
            let groupedDependencyExecutor (policyRecord, onApiRequestProcessingStart) =
                // Pre-compute as much in-advance before we need to do
                // something with the supplied policy record.
                deferredApiCallTasksByRequestor
                |> Map.map (fun _ asyncExecutor ->
                    asyncExecutor (policyRecord, onApiRequestProcessingStart))

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

            fun (policyRecord, onApiRequestProcessingStart) ->
                backgroundTaskResult {
                    let responseTasksByRequestor =
                        groupedDependencyExecutor (policyRecord, onApiRequestProcessingStart)
                            
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

                    let! consolidatedResponses =
                        responsesByRequestor
                        |> Map.toSeq
                        |> Seq.fold apiResponsesConsolidator (Ok Map.empty) 

                    let stepResults =
                        (parsedSources, responseArrayHydrators)
                        ||> List.map2 (fun source hydrator ->
                                let invokerArgs =
                                    // Hydrator requires our grouped API responses.
                                    Array.init source.ApiCalls.Count (hydrator consolidatedResponses)

                                let stepResult =
                                    source.Invoker (policyRecord, invokerArgs)
                                    
                                stepResult)

                    do assert (stepResults.Length = parsedSources.Length)

                    return stepResults
                }

    // This is used for the exited and new records only. This is because such
    // policies only require a single step to be run.
    let private createEvaluatorForStep
        (parsedSource: ParsedSource<'TPolicyRecord, 'TStepResults>) =
            // Running a single step is the same as running a list of just one step!
            let evaluator =
                createEvaluatorForSteps
                    [ parsedSource ]

            fun (policyRecord, onApiRequestProcessingStart)  ->
                backgroundTaskResult {
                    let! stepResult =
                        evaluator (policyRecord, onApiRequestProcessingStart)

                    return List.exactlyOne stepResult
                }                       


    (*
    --- REMAINING POLICY RECORD SPECIFIC LOGIC ---
    The following is logic used to run policy records present at both the opening AND closing steps.
    The logic itself is very complicated... DEAL WITH IT.

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
                result {
                    let! newRecordForDataStage =
                        dcs.DataChanger (openingPolicyRecord, priorPolicyRecord, closingPolicyRecord)
                        |> Result.mapError (fun reasons ->
                            [ WalkEvaluationFailure.DataChangeFailure (dcs, reasons) ])

                    let recordToPassOn =
                        newRecordForDataStage
                        |> Option.defaultValue priorPolicyRecord

                    let! remainingDataStages =
                        inner recordToPassOn dcss 

                    return newRecordForDataStage::remainingDataStages
                }
                
            | [] ->
                Ok [] 
                
        inner openingPolicyRecord


    // These Groupings... types are helpers that allow us to group commonly used values together.
    [<NoEquality; NoComparison>]
    type private GroupingsByDataStage<'TPolicyRecord, 'TStepResults> =
        {
            ParsedSources           : ParsedSource<'TPolicyRecord, 'TStepResults> list list
            StepHeaders             : IStepHeader list list
            StepUids                : Guid list list
            TelemetryDataSources    : StepDataSource list
        }

    // This is to make the distinction explicit, as opposed to the groupings by data stage above.
    [<NoEquality; NoComparison>]
    type private GroupingsByProfile<'TPolicyRecord, 'TStepResults> =
        {       
            ParsedSources           : ParsedSource<'TPolicyRecord, 'TStepResults> list list
            StepHeaders             : IStepHeader list list
            StepUids                : Guid list list
            TelemetryDataSources    : StepDataSource list
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
                        StepDataSource.OpeningData
                    | :? MoveToClosingDataStep<'TPolicyRecord, 'TStepResults> ->
                        StepDataSource.ClosingData
                    | :? DataChangeStep<'TPolicyRecord, 'TStepResults> as hdr ->
                        StepDataSource.DataChangeStep hdr
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
                do assert (xs.Length = profile.Length)

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
                    // within each of our profile groupings.
                    |> List.map List.head
            }


    (*
    Design Decision:
        We have the a complication in that the opening re-run step can be run without any prior step results.
        Arguably, we could have separate execution and validation logic for the opening re-run step.
        However, this would lead to extra complexity for seemingly dogmatic reasons.    

        Note that this logic is also used for new records where the first step run is (not surprisingly) the
        add new records step. This step doesn't require knowledge of the previous step result.
    *)
    let rec private tryFindValidationFailureWithinProfileStage<'TPolicyRecord, 'TStepResults, 'TApiCollection when 'TPolicyRecord: equality>
        (policyRecordForPriorStage, policyRecordForStage, priorStepResults: 'TStepResults option)
        : (IStepHeader * 'TStepResults) list -> _ = function
        | (hdr, currentStepResults)::xs ->
            let stepValidationOutcome =
                match policyRecordForPriorStage, priorStepResults, hdr with
                // The opening re-run step might not have prior closing results available.
                // Regardless, there will NEVER be a prior stage.
                | None, _, (:? OpeningReRunStep<'TPolicyRecord, 'TStepResults, 'TApiCollection> as hdr) ->
                    hdr.Validator (policyRecordForStage, priorStepResults, currentStepResults)
                // For a new record, there won't be a prior stage, let alone any data for it.
                // However, we will have data available for a remaining record.
                // In either case, there will be prior step results available.
                | _, Some priorStepResults', (:? SourceChangeStep<'TPolicyRecord, 'TStepResults, 'TApiCollection> as hdr) ->
                    hdr.Validator (policyRecordForStage, priorStepResults', currentStepResults)
                // This will occur if the data-change step didn't actually lead to a change in data.
                // As such, if there's been no change in data, and (by definition) no change in step
                // results... Why run validation? We would expect prior results to be available, even
                // if we're ignoring them here.
                | None, Some _, :? DataChangeStep<'TPolicyRecord, 'TStepResults> ->
                    StepValidationOutcome.Empty
                | Some policyRecordForPriorStage', Some priorStepResults', (:? DataChangeStep<'TPolicyRecord, 'TStepResults> as hdr) ->
                    hdr.Validator (policyRecordForPriorStage', priorStepResults', policyRecordForStage, currentStepResults)
                // Same situation as for the data change steps above.
                | None, Some _, :? MoveToClosingDataStep<'TPolicyRecord, 'TStepResults> ->
                    StepValidationOutcome.Empty
                // Same situation as for the data change steps above.
                | Some policyRecordForPriorStage', Some priorStepResults', (:? MoveToClosingDataStep<'TPolicyRecord, 'TStepResults> as hdr) ->
                    hdr.Validator (policyRecordForPriorStage', priorStepResults', policyRecordForStage, currentStepResults)                
                // If we're a new record, we won't have prior step results available. In which case,
                // run our validation. We also won't have data for a prior stage.
                | None, None, (:? AddNewRecordsStep<'TPolicyRecord, 'TStepResults> as hdr) ->
                    hdr.Validator (policyRecordForStage, currentStepResults)
                // If we're a remaining record, we will have prior step results available. As such,
                // why run validation for a remaining record? There's no guarantee whether
                // there will have been a prior stage or not. Hence, cannot restrict prior data.
                | _, Some _, :? AddNewRecordsStep<'TPolicyRecord, 'TStepResults> ->
                    StepValidationOutcome.Empty
                // As per the opening re-run step, there is no prior stage and hence no corresponding data.
                | None, Some _, :? RemoveExitedRecordsStep<'TPolicyRecord, 'TStepResults> ->
                    StepValidationOutcome.Empty
                // We do NOT want a catch-all here. In the event another step type gets introduced,
                // I want this to explicitly fail... At least at runtime!
                | _ ->
                    failwith "Unexpected error."

            // We now need to move between our world of possible validation issues to
            // the world of evaluation failures.
            match stepValidationOutcome, xs with
            | StepValidationOutcome.Completed [], [] ->
                // No issues and nothing else left to validate within this stage.
                None
            | StepValidationOutcome.Completed [], xs ->
                // No issues, so carry on.
                tryFindValidationFailureWithinProfileStage<_, _, 'TApiCollection>
                    (policyRecordForPriorStage, policyRecordForStage, Some currentStepResults) xs
            | StepValidationOutcome.Completed issues, _ ->
                // We have some issues, so return them.
                Some [ WalkEvaluationFailure.ValidationFailure (hdr, issues) ]
            | StepValidationOutcome.Aborted reason, _ ->
                // We have an aborted validation, so return it.
                Some [ WalkEvaluationFailure.ValidationAborted (hdr, reason) ]

        | [] ->
            None


    // Short-hand as type inference is sometimes struggling.
    type private DataStageEvaluator<'TPolicyRecord, 'TStepResults> =
        ('TPolicyRecord * (RequestorName -> OnApiRequestProcessingStart))
            -> Task<Result<'TStepResults list, WalkEvaluationFailure list>>


    // Note that priorStepResults is for the prior step, NOT the prior stage. Not to say
    // those results don't come from the final run within the prior stage!
    // Note that by 'profile', we're referring to series of data-stages that may have been
    // combined depending on how/if the policy data has changed as move through the walk.
    // Data-stages where no data change has occurred are grouped together.
    let rec private evaluateProfile<'TPolicyRecord, 'TStepResults, 'TApiCollection when 'TPolicyRecord: equality>
        onApiRequestProcessingStart (policyRecordForPriorStage: 'TPolicyRecord option, priorStepResults: 'TStepResults option) = function
        | (policyRecordForStage: 'TPolicyRecord, (stepHdrsForStage, stageEvaluator: DataStageEvaluator<_, _>, dataSource: StepDataSource))::xs ->
            backgroundTaskResult {
                let! stageResults =
                    stageEvaluator (policyRecordForStage, onApiRequestProcessingStart dataSource)

                let validationIssues =
                    (stepHdrsForStage, stageResults)
                    ||> List.zip
                    // We need to explicitly specify the API collection type as it cannot
                    // be inferred. Failure to do so means that type-based pattern matches will fail.
                    |> tryFindValidationFailureWithinProfileStage<_, _, 'TApiCollection>
                        (policyRecordForPriorStage, policyRecordForStage, priorStepResults)

                do! validationIssues |> Result.requireNoneWith id
              
                // We need to feed this into the processing of the next profile stage.
                let finalStepResultsForGroup =
                    List.last stageResults

                let! remOutcomes =
                    // Feed in the policy data for this group into the next as prior policy data.
                    // Furthermore, we also need to carry forward the results of the final step
                    // for this group.
                    evaluateProfile<_, _, 'TApiCollection>
                        onApiRequestProcessingStart (Some policyRecordForStage, Some finalStepResultsForGroup) xs

                // Inspect our remaining outcomes to see what we actually want to return.
                return stageResults::remOutcomes                   
            }

        | [] ->
            backgroundTaskResult {
                // No more stages to process, so return an empty list.
                return []
            }


    // A short-hand alias used when type inference struggles.
    type private ProfileEvaluator<'TPolicyRecord, 'TStepResults when 'TPolicyRecord: equality> =
        'TPolicyRecord * 'TStepResults option * 'TPolicyRecord list * (StepDataSource -> RequestorName -> OnApiRequestProcessingStart) -> 
            Task<Result<(StepDataSource * 'TStepResults) list, WalkEvaluationFailure list>>


    let private createRemainingPolicyEvaluator<'TPolicyRecord, 'TStepResults, 'TApiCollection when 'TPolicyRecord: equality>
        (logger: ILogger) (parsedWalk: ParsedWalk<'TPolicyRecord, 'TStepResults>) =
            let walkStepUids =
                parsedWalk.ParsedSteps
                |> List.map (fst >> _.Uid)

            // Pre-compute this once up-front.
            let groupingsByDataStage =
                // If we don't pass in the API collection type, obj gets inferred.
                getGroupingsByDataStage<_, _, 'TApiCollection> parsedWalk

            let postOpeningDataChangeSteps =
                parsedWalk.PostOpeningDataStages
                |> List.map _.DataChangeStep

            // Note that value factories for a concurrent dictionary CAN be run in parallel.
            // However, only the first to complete will be retained.
            // As such, where we have value factory logic referring to a dictionary,
            // that dictionary MUST also be concurrent!
            let cachedGroupingsForProfile =
                new ConcurrentDictionary<int list, GroupingsByProfile<'TPolicyRecord, 'TStepResults>> ()

            let cachedEvaluatorsForUids =
                new ConcurrentDictionary<Guid list, DataStageEvaluator<'TPolicyRecord, 'TStepResults>> ()

            let cachedProfileEvaluators =
                new ConcurrentDictionary<int list, ProfileEvaluator<'TPolicyRecord, 'TStepResults>> ()

            let createProfileEvaluator groupingProfileIdxs : ProfileEvaluator<'TPolicyRecord, 'TStepResults> =
                let groupingsForProfile =
                    cachedGroupingsForProfile.GetOrAdd (groupingProfileIdxs, fun _ ->
                        //do logger.LogDebug (sprintf "Creating groupings by profile for %A." groupingProfileIdxs)

                        getGroupingsByProfile groupingsByDataStage groupingProfileIdxs)

                let evaluatorsByProfileStage =
                    (groupingsForProfile.StepUids, groupingsForProfile.ParsedSources)
                    ||> List.map2 (fun uids sources ->
                            //do logger.LogDebug
                            //    (sprintf "Creating executor for %ix step UIDs starting with %O."
                            //        uids.Length groupingProfileIdxs.Head)

                            // Again, ensure cached versions are used where available.
                            cachedEvaluatorsForUids.GetOrAdd (uids, fun _ -> createEvaluatorForSteps sources))

                let profileStagesInfo =
                    (groupingsForProfile.StepHeaders, evaluatorsByProfileStage, groupingsForProfile.TelemetryDataSources)
                    |||> List.zip3

                let dataSourceByStep =
                    (groupingsForProfile.StepHeaders, groupingsForProfile.TelemetryDataSources)
                    ||> List.map2 (fun headers dataSource ->
                            List.replicate headers.Length dataSource)
                    |> List.concat

                do assert (dataSourceByStep.Length = parsedWalk.ParsedSteps.Length)                                

                //do logger.LogDebug
                //    (sprintf "Created executor for profile %A on thread #%i."
                //        groupingProfileIdxs
                //        Environment.CurrentManagedThreadId)

                // We CANNOT refer to the outer policy record information as this will create
                // a closure to stale inputs.
                fun (openingPolicyRecord', priorClosingStepResults', policyRecordsProfile': _ list, onApiRequestProcessingStart) ->
                    backgroundTaskResult {
                        do assert (policyRecordsProfile'.Length = profileStagesInfo.Length)

                        let! walkResults =
                            // We can't compute this ahead of time as it depends on the
                            // policy records being processed.
                            (policyRecordsProfile', profileStagesInfo)
                            ||> List.zip
                            // Again, the need to pass in the API collection type!
                            |> evaluateProfile<_, _, 'TApiCollection>
                                // There is no prior-stage and hence no such data.
                                onApiRequestProcessingStart (None, priorClosingStepResults')

                        let walkResults' =
                            walkResults
                            // Flatten our results...
                            |> List.concat
                            // ...and then stitch these with the data source for each step.
                            |> List.zip dataSourceByStep

                        return walkResults'
                    }

            fun (RemainingPolicy (openingPolicyRecord, closingPolicyRecord), priorClosingStepResults, onApiRequestProcessingStart) ->
                backgroundTaskResult {
                    let! postOpeningDataChanges =
                        extractPostOpeningDataChanges
                            (openingPolicyRecord, closingPolicyRecord) postOpeningDataChangeSteps

                    do assert (postOpeningDataChanges.Length = parsedWalk.PostOpeningDataStages.Length)
                       
                    // Here we create a list of indexes which indicate the grouping of data stages.
                    let groupingProfileIdxs =
                        postOpeningDataChanges
                        |> List.scan (fun priorIdx -> function
                            | Some _    -> priorIdx + 1
                            | None      -> priorIdx) 0
                        // Don't drop the initial state as this corresponds to the opening data stage.

                    let policyRecordsForProfile =
                        postOpeningDataChanges
                        |> List.choose id
                        // Bring in the opening policy record which coresponds to grouping idx #0.
                        |> List.append [ openingPolicyRecord ]

                    let interiorDataChanges =
                        (postOpeningDataChangeSteps, postOpeningDataChanges)
                        ||> Seq.zip
                        |> Seq.choose (function
                            // We only want the interior data changes. We don't care about the opening or
                            // closing stages as 
                            | :? DataChangeStep<'TPolicyRecord, 'TStepResults> as hdr, Some record ->
                                Some (hdr.Uid, record)
                            | _ ->
                                None)
                        |> Map.ofSeq

                    let profileEvaluator =
                        // Make sure we load a cached version where available.
                        // TODO - This is going to lead to allocations where we're referring to the
                        // prevailing profile via a closure. However, for aesthetics if nothing else,
                        // I'm inclined to keep it as it is. Right?
                        cachedProfileEvaluators.GetOrAdd (groupingProfileIdxs, createProfileEvaluator)

                    let! walkResults =
                        profileEvaluator
                            (openingPolicyRecord, priorClosingStepResults, policyRecordsForProfile, onApiRequestProcessingStart)                                                      

                    let output =                            
                        {
                            InteriorDataChanges =
                                interiorDataChanges
                            StepResults         =
                                walkResults
                                |> Seq.zip walkStepUids
                                |> Map.ofSeq
                        }

                    return output
                }

    // ------ END OF REMAINING POLICY RECORD SPECIFIC LOGIC ------
                

    // As seen before, we're only asking for the API collection
    // type for the purpose of type assertion at run-time and/or pattern matching.
    let private createExitedPolicyEvaluator<'TPolicyRecord, 'TStepResults, 'TApiCollection>
        (parsedWalk: ParsedWalk<'TPolicyRecord, 'TStepResults>) =
            let openingReRunStepHdr, openingReRunParsedStep =
                // This CANNOT fail! If it does, I've not idea
                // how the logic managed to get this far!
                List.head parsedWalk.ParsedSteps

            // Let's just make sure we're dealing with the step we're actually expecting!
            do assert checkStepType<OpeningReRunStep<'TPolicyRecord, 'TStepResults, 'TApiCollection>> openingReRunStepHdr

            let openingReRunStep : OpeningReRunStep<'TPolicyRecord, 'TStepResults, 'TApiCollection> =
                downcast openingReRunStepHdr

            let evaluator =
                createEvaluatorForStep openingReRunParsedStep

            fun (ExitedPolicy policyRecord, priorClosingStepResults, onApiRequestProcessingStart) ->
                backgroundTaskResult {
                    let! stepResults =
                        evaluator (policyRecord, onApiRequestProcessingStart StepDataSource.OpeningData)
                
                    let validationOutcome =
                        openingReRunStep.Validator (policyRecord, priorClosingStepResults, stepResults)

                    match validationOutcome with
                    | StepValidationOutcome.Completed [] ->
                        let evaluationOutcome : EvaluatedPolicyWalk<'TPolicyRecord, _> =
                            {
                                InteriorDataChanges =
                                    Map.empty
                                StepResults         =
                                    Map.singleton
                                        openingReRunStepHdr.Uid
                                        (StepDataSource.OpeningData, stepResults)
                            }

                        return evaluationOutcome

                    | StepValidationOutcome.Completed issues ->
                        return! Error [ WalkEvaluationFailure.ValidationFailure
                            (openingReRunStepHdr, issues) ]

                    | StepValidationOutcome.Aborted reason ->
                        return! Error [ WalkEvaluationFailure.ValidationAborted
                            (openingReRunStepHdr, reason) ]
                }

    // Note the comments for the exited policy evaluator above.
    let private createNewPolicyEvaluator<'TPolicyRecord, 'TStepResults, 'TApiCollection when 'TPolicyRecord: equality>
        (parsedWalk: ParsedWalk<'TPolicyRecord, 'TStepResults>) =
            // Here we check to make sure that we can find the add new records step.
            let newRecordsStepIdx =
                parsedWalk.ParsedSteps
                |> List.findIndex (function
                    | :? AddNewRecordsStep<'TPolicyRecord, 'TStepResults>, _ -> true | _ -> false)

            // This will include the add new records step above.
            let parsedStepsForNewRecords =
                parsedWalk.ParsedSteps
                |> List.skip newRecordsStepIdx

            // Make sure we're dealing with the step we think we are!
            do assert checkStepType<AddNewRecordsStep<'TPolicyRecord, 'TStepResults>>
                (fst parsedStepsForNewRecords.Head)

            // Here we ensure that anything after the add new records step 
            // is a source change step.
            do  parsedStepsForNewRecords
                |> List.tail
                |> List.iter (fun (stepHdr, _) ->
                    do assert checkStepType<SourceChangeStep<'TPolicyRecord, 'TStepResults, 'TApiCollection>> stepHdr)

            let stepHeadersForNewRecords =
                parsedStepsForNewRecords
                |> List.map fst

            let stepUidsForNewRecords =
                stepHeadersForNewRecords
                |> List.map _.Uid

            let parsedSourcesForNewRecords =
                parsedStepsForNewRecords                
                |> List.map snd

            let evaluator =
                createEvaluatorForSteps parsedSourcesForNewRecords

            fun (NewPolicy policyRecord, onApiRequestProcessingStart) ->
                backgroundTaskResult {
                    let! stepResults =
                        evaluator (policyRecord, onApiRequestProcessingStart StepDataSource.ClosingData)

                    let validationIssues =
                        (stepHeadersForNewRecords, stepResults)
                        ||> List.zip
                        |> tryFindValidationFailureWithinProfileStage<_, _, 'TApiCollection>
                            // There is no prior stage for a new record. Hence, no corresponding data
                            // no prior step results.
                            (None, policyRecord, None)

                    do! validationIssues |> Result.requireNoneWith id

                    // We need the type hint or the policy type can't be infered.
                    let evaluatedWalk : EvaluatedPolicyWalk<'TPolicyRecord, _> =
                        {
                            InteriorDataChanges =
                                Map.empty
                            StepResults         =
                                stepResults
                                |> Seq.map (fun sr -> StepDataSource.ClosingData, sr)
                                |> Seq.zip stepUidsForNewRecords
                                |> Map.ofSeq                            
                        }

                    return evaluatedWalk
                }


    let create
        (logger: ILogger)
        (walk: AbstractWalk<_, _, 'TApiCollection>)
        (apiCollection) =
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

            let nullDisposable =
                {
                    new IDisposable with member _.Dispose () = ()
                }

            // TODO - Even this could lead to allocations galore.
            let nullTelemetryCallback _ _ _ =
                nullDisposable

            {
                new IPolicyWalkEvaluator<'TPolicyRecord, 'TStepResults> with

                    member _.Execute (ExitedPolicy _ as exitedPolicyRecord, priorClosingStepResults) =
                        exitedRecordEvaluator (exitedPolicyRecord, priorClosingStepResults, nullTelemetryCallback)

                    member _.Execute (ExitedPolicy _ as exitedPolicyRecord, priorClosingStepResults, onApiRequestProcessingStart) =
                        exitedRecordEvaluator (exitedPolicyRecord, priorClosingStepResults, onApiRequestProcessingStart)

                    member _.Execute (RemainingPolicy _ as remainingPolicy, priorClosingStepResults) =
                        remainingRecordEvaluator (remainingPolicy, priorClosingStepResults, nullTelemetryCallback)

                    member _.Execute (RemainingPolicy _ as remainingPolicy, priorClosingStepResults, onApiRequestProcessingStart) =
                        remainingRecordEvaluator (remainingPolicy, priorClosingStepResults, onApiRequestProcessingStart)

                    member _.Execute (NewPolicy _ as newPolicyRecord) =
                        newRecordEvaluator (newPolicyRecord, nullTelemetryCallback)

                    member _.Execute (NewPolicy _ as newPolicyRecord, onApiRequestProcessingStart) =
                        newRecordEvaluator (newPolicyRecord, onApiRequestProcessingStart)
            }


namespace AnalysisOfChangeEngine.Controller

open System
open System.Threading
open System.Threading.Tasks
open System.Threading.Tasks.Dataflow
open AnalysisOfChangeEngine    
        



[<RequireQualifiedAccess>]
type internal CohortMembership<'T, 'U> =
    | Exited of 'T
    | Remaining of 'U
    | New of 'T

(*
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
                                | Some (Ok record) -> 
                                    Some (CohortMembership.Exited record)
                                | Some (Error reason) ->
                                    do notifyRecordReadFailure (pid, reason)
                                    None
                                | None ->
                                    do notifyRecordNotFound pid
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

(*

let private createEvaluatorsForProfile
        (logger: ILogger)
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
                |> List.append
                    [ parsedWalk.OpeningDataStage.WithinStageSteps, TelemetryDataSource.OpeningData ]

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
                            do logger.LogDebug
                                (sprintf "Creating executor for %i steps starting with %O." stepUids.Length stepUids[0])

                            let executor =
                                createExecutorForSteps parsedSources

                            fun policyRecord ->
                                backgroundTask {
                                    let! groupOutcome, taggedTelemetry =
                                        executor policyRecord

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

    let rec private tryFindGroupValidationFailure<'TPolicyRecord, 'TStepResults, 'TApiCollection when 'TPolicyRecord: equality>
        (priorGroupPolicyRecord, currentGroupPolicyRecord, priorStepResults)
        : (IStepHeader * 'TStepResults) list -> _ = function
        | (hdr, currentStepResults)::xs ->
            let stepValidationOutcome =
                match priorStepResults, hdr with
                | _, (:? OpeningReRunStep<'TPolicyRecord, 'TStepResults, 'TApiCollection> as hdr) ->
                    hdr.Validator (currentGroupPolicyRecord, priorStepResults, currentStepResults)
                | Some priorStepResults', (:? SourceChangeStep<'TPolicyRecord, 'TStepResults, 'TApiCollection> as hdr) ->
                    hdr.Validator (currentGroupPolicyRecord, priorStepResults', currentStepResults)
                | Some priorStepResults', (:? DataChangeStep<'TPolicyRecord, 'TStepResults> as hdr) ->
                    hdr.Validator (priorGroupPolicyRecord, priorStepResults', currentGroupPolicyRecord, currentStepResults)
                | Some priorStepResults', (:? MoveToClosingDataStep<'TPolicyRecord, 'TStepResults> as hdr) ->
                    hdr.Validator (priorGroupPolicyRecord, priorStepResults', currentGroupPolicyRecord, currentStepResults)
                | Some _, (:? AddNewRecordsStep<'TPolicyRecord, 'TStepResults> as hdr) ->
                    hdr.Validator (currentGroupPolicyRecord, currentStepResults)
                | Some _, :? RemoveExitedRecordsStep<'TPolicyRecord, 'TStepResults> ->
                    StepValidationOutcome.Empty
                | _ ->
                    failwith "Unexpected error."

            match stepValidationOutcome, xs with
            | StepValidationOutcome.Completed [||], [] ->
                // No issues, so carry on.
                None
            | StepValidationOutcome.Completed [||], xs ->
                tryFindGroupValidationFailure
                    (priorGroupPolicyRecord, currentGroupPolicyRecord, Some currentStepResults) xs
            | StepValidationOutcome.Completed issues, _ ->
                // We have some issues, so return them.
                Some (EvaluationFailure.ValidationFailure (hdr, issues))
            | StepValidationOutcome.Aborted reason, _ ->
                // We have an aborted validation, so return it.
                Some (EvaluationFailure.ValidationAborted (hdr, [| reason |]))

        | [] ->
            None


    let rec private executeDataStageGroups<'TPolicyRecord, 'TStepResults, 'TApiCollection when 'TPolicyRecord: equality>
        (priorGroupPolicyRecord, priorGroupClosingResults)
        (groupedRecordsAndStepsAndExecutors: ('TPolicyRecord * IStepHeader list * DataStageExecutor<'TPolicyRecord, 'TStepResults>) list) =
            match groupedRecordsAndStepsAndExecutors with
            | (policyRecord, stepHdrs, executor)::xs ->
                backgroundTask {
                    let! dataStageOutcome, telemetry =
                        executor policyRecord

                    let! groupsOutcomes =
                        match dataStageOutcome with
                        | Ok dataStageResults ->
                            let validationIssues =
                                (stepHdrs, dataStageResults)
                                ||> List.zip
                                |> tryFindGroupValidationFailure (priorGroupPolicyRecord, policyRecord, priorGroupClosingResults)

                            match validationIssues with
                            | Some failure ->
                                backgroundTask {
                                    return Error [failure], [telemetry]
                                }                                

                            | None ->
                                backgroundTask {                               
                                    let finalStepResultsForGroup =
                                        List.last dataStageResults

                                    let! nextOutcomes =
                                        // Feed in the policy data for this group into the next as prior policy data.
                                        // Furthermore, we also need to carry forward the results of the final step
                                        // for this group.
                                        executeDataStageGroups (policyRecord, Some finalStepResultsForGroup) xs

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

                    return groupsOutcomes                            
                }
            | [] ->
                backgroundTask {
                    return Ok [[]], [[]]
                }

    // We intentionally refer to this as an evaluator, particularly given it's returning
    // telemetry for the evaluation itself.
    let private createRemainingPolicyEvaluator<'TPolicyRecord, 'TStepResults when 'TPolicyRecord: equality>
        (logger: ILogger)
        (parsedWalk: ParsedWalk<'TPolicyRecord, 'TStepResults>) =
            (*
            Design Decision:
                For a given record, we could process all data stages in one go.
                However, if a data stage is going to fail, it is better we find
                this out as soon as possible so we don't waste time making
                API calls that will just be discarded anyway.
            *)

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
                |> List.append
                    [ parsedWalk.OpeningDataStage.WithinStageSteps, TelemetryDataSource.OpeningData ]

            let cachedProfiles =
                // This MUST be concurrent given we'll have multiple records in flight
                // at the same time.
                new ConcurrentDictionary<int list, _> ()

            let groupedEvaluatorsFactory =
                createEvaluatorsForProfile logger parsedWalk
            
            fun (RemainingPolicy (openingPolicyRecord, closingPolicyRecord), priorClosingStepResults: 'TStepResults option) ->   
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
                       
                        // Here we create a list of indexes which indicate the grouping of data stages.
                        let dataStageGroupingIdxs =
                            postOpeningDataChanges'
                            |> List.scan (fun priorIdx -> function
                                | Some _    -> priorIdx + 1
                                | None      -> priorIdx) 0
                            // Don't drop the initial state as that corresponds to the opening data stage.

                        // For each of the distinct groups above, figure out the data we want to use.
                        let policyDataByGroup =
                            postOpeningDataChanges'
                            // Convert our list of maybe records into a list of, well, records.
                            |> List.choose id
                            // Bring in the opening policy record.
                            |> List.append [ openingPolicyRecord ]   
                            
                        let profileGroupings =
                            cachedProfiles.GetOrAdd
                                (dataStageGroupingIdxs, fun _ ->
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

                                    let groupedStepHeaders =
                                        groupedParsedSteps
                                        |> List.map (fst >> List.map fst)
                                    
                                    let groupedUids =
                                        groupedStepHeaders
                                        |> List.map (List.map _.Uid)

                                    let groupedParsedSources =
                                        groupedParsedSteps
                                        |> List.map (fun (parsedSteps, dataSource) ->
                                            let parsedSources =
                                                parsedSteps |> List.map snd

                                            parsedSources, dataSource)
                                    
                                )

                        let evaluatorsByGroup =
                            // Make sure we re-use any cached evaluators for this given profile.
                            cachedGroupedEvaluators.GetOrAdd
                                (dataStageGroupingIdxs, fun idxs ->
                                    do logger.LogDebug (sprintf "Create evaluator for grouping %A." idxs)

                                    groupedEvaluatorsFactory idxs)

                        assert (policyDataByGroup.Length = evaluatorsByGroup.Length)

                        let! allStepResults, evaluationApiTelemetry =
                            (policyDataByGroup, stepHeadersByGroup, evaluatorsByGroup)
                            |||> List.zip3
                            |> executeDataStageGroups (openingPolicyRecord, priorClosingStepResults)                    

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
                        // Although we've failed, we've still used up some time to this point.
                        let evaluationTelemetry =
                            {
                                EvaluationStart     = evaluationStart
                                EvaluationEnd       = DateTime.Now
                                ApiRequestTelemetry = []
                            }

                        return Error [ reason ], evaluationTelemetry
                    }


*)
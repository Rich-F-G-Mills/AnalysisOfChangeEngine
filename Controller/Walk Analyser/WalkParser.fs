
namespace AnalysisOfChangeEngine.Controller.WalkAnalyser


[<RequireQualifiedAccess>]
module (*internal*) WalkParser =

    open FSharp.Reflection
    open FSharp.Quotations
    open AnalysisOfChangeEngine
    open AnalysisOfChangeEngine.Common.StateMonad


    type private WalkState<'TPolicyRecord> =
        Map<SourceElementApiCallDependency<'TPolicyRecord>, Var> * Map<string, SourceElementDefinition<'TPolicyRecord>>
    
    type private SourceParser<'TPolicyRecord, 'TStepResults, 'TApiCollection> =
        SourceExpr<'TPolicyRecord, 'TStepResults, 'TApiCollection>
            -> Stateful<WalkState<'TPolicyRecord>, ParsedSource<'TPolicyRecord, 'TStepResults>>


    (*
    Design Decision:
        We Process each sourceable step in turn. This includes processing the underlying source expression,
        and switching out any references to API calls and/or results from the current step with variables.
        When we process each sourceable step (and the step result elements within), we track (via a state monad)
        an accumulated view of elements defined from previous steps. That is, before processing a given step, we know
        the latest definition of each element.

        Furthermore, within each step at least, we need a way to map each API call to a corresponding variable
        definition (in a 1-2-1 manner). The current source parsing logic only does this mapping for those
        elements which are explicitly defined for a given step. It does NOT consider those parsed elements
        inherited from previous steps.

        Rather than re-parse inherited element definitions, it seemed cleaner to track all API calls encountered
        across all steps encountered thus far, each associated with a corresponding variable. This is why, via
        the state monad below, we track an accumulated view for API call/variable mappings.
    *)

    let private getParsedStepSources<'TPolicyRecord, 'TStepResults, 'TApiCollection>
        (steps: IStepHeader list, sourceParser: SourceParser<'TPolicyRecord, 'TStepResults, 'TApiCollection>) =
            // First of all, lets figure out which steps are sourceable.
            let sourceableStepHdrs =
                steps
                |> List.choose (function
                    | :? ISourceableStep<'TPolicyRecord, 'TStepResults, 'TApiCollection> as step ->
                        Some (step.Uid, step.Source)
                    | _ ->
                        None)

            // We don't care about the state returned from the run below.
            let parsedSources, _ =
                sourceableStepHdrs
                |> List.map snd
                |> List.mapStateM sourceParser
                |> Stateful.run (Map.empty, Map.empty)
                
            // For those sourceable steps, create a mapping between the source step UIDs and the
            // corresponding source.
            let parsedStepMapping =
                (sourceableStepHdrs, parsedSources)
                ||> List.map2 (fun (uid, _) parsed -> uid, parsed)
                |> Map.ofList

            let firstStepUid =
                steps
                |> List.head
                |> _.Uid

            // It would be very (!) unexpected if this fails given the above.
            let firstParsedStepSource =
                parsedStepMapping[firstStepUid]

            // Now we bring together a list of parsed sources for all post-opening steps.
            // If a step is non-sourceable, we just carry forward that from the previous step.
            let parsedStepSources =
                steps
                // Ignore the first post-opening step as we've already determined
                // the corresponding parsed source above. Note that scan returns
                // the initial state as part of the resulting collection.
                |> List.tail
                |> List.scan (fun prior step ->
                    match parsedStepMapping.TryFind step.Uid with
                    | Some source -> source
                    // If we can't find a source for this uid, reuse the previous one.
                    | None -> prior) firstParsedStepSource

            assert (steps.Length = parsedStepSources.Length)

            parsedStepSources


    let private getOpeningDataStage (openingDataStageTuples) =
        {
            WithinStageSteps =
                openingDataStageTuples
        }

    let private getPostOpeningDataStages (postOpeningDataStageTuples: (IStepHeader * _) list) =
        // If nothing else, we would have encountered the move to closing existing data step!
        assert (postOpeningDataStageTuples.Length > 0)

        let firstPostOpeningDataChangeStepHeader =
            postOpeningDataStageTuples
            |> List.head
            |> fst
            // If this fails, something has gone very (!) wrong within the caller.
            :?> IDataChangeStep<'TPolicyRecord>

        let dataStageAccumulator accumStage (step: IStepHeader) =
            match step with
            | :? IDataChangeStep<'TPolicyRecord> as step' ->
                step'
            | _ ->
                // If we aren't a data change step, we must be within the same data-stage.
                accumStage

        let postOpeningDataStagesByStep =
            postOpeningDataStageTuples
            // Skip the first one as we're passing it in as the initial scan state.
            |> List.tail
            |> List.map fst
            |> List.scan dataStageAccumulator firstPostOpeningDataChangeStepHeader

        assert (postOpeningDataStagesByStep.Length = postOpeningDataStageTuples.Length)

        let stepsGroupedByDataStage =
            postOpeningDataStageTuples
            // Not clear whether groupBy preserves ordering. Easier just to maintain
            // it ourselves.
            |> List.indexed
            |> List.zip postOpeningDataStagesByStep
            |> List.groupBy fst
            |> List.map (fun (stage, details) ->
                let details' =
                    details
                    |> List.map snd
                    |> List.sortBy fst
                    |> List.map snd

                stage, details')

        // Note that withinStageTuples will also include the data change step itself.
        // Design Decision: Could be moved out of this definition? However, not seeing any benefit.
        let toDataStage (dataChangeStepHdr: IDataChangeStep<_>, withinStageTuples) =
            {
                DataChangeStep =
                    dataChangeStepHdr
                WithinStageSteps =
                    // This DOES include the data change step itself.
                    withinStageTuples
            }

        let postOpeningDataStages =
            stepsGroupedByDataStage
            |> List.map toDataStage

        postOpeningDataStages
        

    let (*internal*) execute<'TPolicyRecord, 'TStepResults, 'TApiCollection when 'TPolicyRecord: equality>
        (walk: AbstractWalk<'TPolicyRecord, 'TStepResults, 'TApiCollection>)
        (apiCollection: 'TApiCollection) =
            let currentResultsVarDefMapping =
                FSharpType.GetRecordFields (typeof<'TStepResults>)
                |> Seq.map (fun pi ->
                    let varName =
                        "CurrentResult.<" + pi.Name + ">"

                    pi.Name, Var (varName, pi.PropertyType))
                |> Map.ofSeq 
            
            let allSteps =
                Seq.toList walk.AllSteps                   

            // Let's make sure our view of how the walk is structured correctly!
            // There is the risk that the required steps under the abstract walk are changed
            // and not correctly reflected here.
            assert checkStepType<OpeningReRunStep<'TPolicyRecord, 'TStepResults, 'TApiCollection>> allSteps[0]
            assert checkStepType<RemoveExitedRecordsStep> allSteps[1]

            let moveToClosingDataStepIdx =
                allSteps
                // We don't use 'try' here as failure to find this step would be a disaster!
                |> List.findIndex (function
                    | :? MoveToClosingDataStep<'TPolicyRecord, 'TStepResults> -> true | _ -> false)

            // We've already checked the presence of the move to closing step above.
            // Now we need to make sure the following step is the addition of new records.
            assert checkStepType<AddNewRecordsStep<'TPolicyRecord, 'TStepResults>> allSteps[moveToClosingDataStepIdx + 1]

            // Finally, check that any step post-inclusion of new records are all source change steps.
            do  allSteps
                // This will exclude both of the move to closing data AND add new records steps above.
                |> List.skip (moveToClosingDataStepIdx + 2)
                |> List.iter (fun stepHdr ->
                    do assert checkStepType<SourceChangeStep<'TPolicyRecord, 'TStepResults, 'TApiCollection>> stepHdr)

            let uniqueStepUids =
                allSteps
                |> Seq.map _.Uid
                |> Set

            assert (allSteps.Length = uniqueStepUids.Count)

            let policyRecordVarDef =
                Var ("policyRecord", typeof<'TPolicyRecord>)

            let sourceParser =
                SourceParser.execute<'TPolicyRecord, 'TStepResults, 'TApiCollection>
                    (apiCollection, policyRecordVarDef, currentResultsVarDefMapping)

            let parsedStepSources =
                getParsedStepSources (allSteps, sourceParser)

            // It's more convenient to keep the step and parsed source together in this way.
            /// Does not include the opening step.
            let parsedStepTuples =
                (allSteps, parsedStepSources)
                ||> List.zip 

            assert (allSteps.Length = parsedStepTuples.Length)

            let isOpeningDataStageIndicator =
                allSteps
                |> List.scan (fun priorInd step ->
                    match priorInd, step with
                    // We're still in the opening data stage until we encounter
                    // our first post-opening data change.
                    | true, :? IDataChangeStep<'TPolicyRecord> -> false
                    | ind, _ -> ind) true
                // Drop the indicator for the opening step.
                |> List.tail

            assert (allSteps.Length = isOpeningDataStageIndicator.Length)

            let openingDataStageTuples, postOpeningDataStageTuples =
                (isOpeningDataStageIndicator, parsedStepTuples)
                ||> List.zip 
                |> List.partition fst
                |> function
                    | opening, postOpening ->
                        let opening' =
                            opening |> List.map snd

                        let postOpening' =
                            postOpening |> List.map snd

                        opening', postOpening'

            // The move to closing existing data stage will ensure this cannot be empty.
            assert (postOpeningDataStageTuples.Length > 0)
            assert (allSteps.Length = openingDataStageTuples.Length + postOpeningDataStageTuples.Length)


            let parsedWalk =
                {
                    ParsedSteps =
                        parsedStepTuples
                    OpeningDataStage =
                        getOpeningDataStage openingDataStageTuples
                    PostOpeningDataStages =
                        getPostOpeningDataStages postOpeningDataStageTuples
                }

            let impliedCountSteps =
                + parsedWalk.OpeningDataStage.WithinStageSteps.Length
                + (List.sumBy _.WithinStageSteps.Length parsedWalk.PostOpeningDataStages)

            assert (impliedCountSteps = allSteps.Length)

            parsedWalk

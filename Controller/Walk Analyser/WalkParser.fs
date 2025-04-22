
namespace AnalysisOfChangeEngine.Controller.WalkAnalyser


[<RequireQualifiedAccess>]
module WalkParser =

    open FSharp.Reflection
    open FSharp.Quotations
    open AnalysisOfChangeEngine
    open AnalysisOfChangeEngine.StateMonad


    type private WalkState<'TPolicyRecord> =
        Map<SourceElementApiCallDependency<'TPolicyRecord>, Var> * Map<string, SourceElementDefinition<'TPolicyRecord>>
    
    type private SourceParser<'TPolicyRecord, 'TStepResults, 'TApiCollection> =
        SourceExpr<'TPolicyRecord, 'TStepResults, 'TApiCollection>
            -> Stateful<WalkState<'TPolicyRecord>, ParsedSource<'TPolicyRecord>>


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
        the state monad below, we also track an accumulated view for API call/variable mappings as well.
    *)

    let private getParsedStepSources<'TPolicyRecord, 'TStepResults, 'TApiCollection>
        (stepsPostOpening: IStepHeader list, sourceParser: SourceParser<'TPolicyRecord, 'TStepResults, 'TApiCollection>) =
            // First of all, lets figure out which steps are sourceable.
            let sourceableStepHdrs =
                stepsPostOpening
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
                parsedSources
                |> List.zip sourceableStepHdrs
                |> List.map (fun ((uid, _), parsed) -> uid, parsed)
                |> Map.ofList

            let firstPostOpeningStepUid =
                stepsPostOpening
                |> List.head
                |> _.Uid

            // It would be very (!) unexpected if this fails given the above.
            let firstPostOpeningParsedStepSource =
                parsedStepMapping[firstPostOpeningStepUid]

            // Now we bring together a list of parsed sources for all post-opening steps.
            // If a step is non-sourceable, we just carry forward that from the previous step.
            let parsedStepSources =
                stepsPostOpening
                // Ignore the first post-opening step as we've already determined
                // the corresponding parsed source above. Note that scan returns
                // the initial state as part of the resulting collection.
                |> List.tail
                |> List.scan (fun prior step ->
                    match parsedStepMapping.TryFind step.Uid with
                    | Some source -> source
                    // If we can't find a source for this uid, reuse the previous one.
                    | None -> prior) firstPostOpeningParsedStepSource

            assert (stepsPostOpening.Length = parsedStepSources.Length)

            parsedStepSources


    let private getOpeningDataStage (openingStep, openingDataStageTuples) =
        {
            OpeningStepHeader =
                openingStep
            WithinStageSteps =
                openingDataStageTuples
            WithinStageApiCalls =
                openingDataStageTuples
                |> List.map snd
                |> List.map _.ApiCalls
                |> Set.unionMany
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
                DataChangeStepParsedSource =
                    withinStageTuples
                    |> List.head
                    |> snd
                WithinStageSteps =
                    // We don't want the data change step itself.
                    withinStageTuples
                    |> List.tail                                    
                WithinStageApiCalls =
                    // Remember that the data change step inherits the source
                    // from the previous step. As such, we need to include any API
                    // calls relevant for the data change step itself.
                    withinStageTuples
                    |> List.map snd
                    |> Seq.map _.ApiCalls
                    |> Set.unionMany
            }

        let postOpeningDataStages =
            stepsGroupedByDataStage
            |> List.map toDataStage

        postOpeningDataStages

    let private checkStepType<'TStep when 'TStep :> IStepHeader> (hdr: IStepHeader) =
        match hdr with
        | :? 'TStep -> true
        | _ -> false
        

    let execute<'TPolicyRecord, 'TStepResults, 'TApiCollection when 'TPolicyRecord :> IPolicyRecord and 'TPolicyRecord : equality>
        (apiCollection: 'TApiCollection) (walk: AbstractWalk<'TPolicyRecord, 'TStepResults, 'TApiCollection>) =
            let currentResultsVarDefMapping =
                FSharpType.GetRecordFields (typeof<'TStepResults>)
                |> Seq.map (fun pi ->
                    let varName =
                        "CurrentResult.<" + pi.Name + ">"

                    pi.Name, Var (varName, pi.PropertyType))
                |> Map.ofSeq 
            
            let allSteps =
                Seq.toList walk.AllSteps

            let allStepsRev =
                List.rev allSteps                       

            // Let's make sure our view of how the walk is structured correctly!
            // There is the risk that the required steps under the abstract walk are changed
            // and not correctly reflected here.
            assert checkStepType<OpeningStep<'TPolicyRecord, 'TStepResults>> allSteps[0]
            assert checkStepType<OpeningReRunStep<'TPolicyRecord, 'TStepResults, 'TApiCollection>> allSteps[1]
            assert checkStepType<RemoveExitedRecordsStep<'TPolicyRecord, 'TStepResults>> allSteps[2]

            // TODO - Use 'from-end' indexing?            
            assert checkStepType<MoveToClosingDataStep<'TPolicyRecord, 'TStepResults>> allStepsRev[1]
            assert checkStepType<AddNewRecordsStep<'TPolicyRecord, 'TStepResults>> allStepsRev[0]


            let uniqueStepUids =
                allSteps
                |> Seq.map _.Uid
                |> Set

            assert (allSteps.Length = uniqueStepUids.Count)

            let stepsPostOpening =
                allSteps
                // We don't care about the opening step here.
                |> List.tail

            let policyRecordVarDef =
                Var ("policyRecord", typeof<'TPolicyRecord>)

            let sourceParser =
                SourceParser.execute<'TPolicyRecord, 'TStepResults, 'TApiCollection>
                    (apiCollection, policyRecordVarDef, currentResultsVarDefMapping)

            let parsedStepSources =
                getParsedStepSources (stepsPostOpening, sourceParser)

            // It's more convenient to keep the step and parsed source together in this way.
            /// Does not include the opening step.
            let parsedStepTuples =
                parsedStepSources
                |> List.zip stepsPostOpening

            assert (stepsPostOpening.Length = parsedStepTuples.Length)

            let isOpeningDataStageIndicator =
                stepsPostOpening
                |> List.scan (fun priorInd step ->
                    match priorInd, step with
                    // We're still in the opening data stage until we encounter
                    // our first post-opening data change.
                    | true, :? IDataChangeStep<'TPolicyRecord> -> false
                    | ind, _ -> ind) true
                // Drop the indicator for the opening step.
                |> List.tail

            assert (stepsPostOpening.Length = isOpeningDataStageIndicator.Length)

            let openingDataStageTuples, postOpeningDataStageTuples =
                parsedStepTuples
                |> List.zip isOpeningDataStageIndicator
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
            assert (stepsPostOpening.Length = openingDataStageTuples.Length + postOpeningDataStageTuples.Length)


            let parsedWalk =
                {
                    PostOpeningParsedSteps =
                        postOpeningDataStageTuples
                    OpeningDataStage =
                        getOpeningDataStage (allSteps[0], openingDataStageTuples)
                    PostOpeningDataStages =
                        getPostOpeningDataStages postOpeningDataStageTuples
                }

            let impliedCountSteps =
                1 + parsedWalk.OpeningDataStage.WithinStageSteps.Length
                  + parsedWalk.PostOpeningDataStages.Length
                  + (List.sumBy _.WithinStageSteps.Length parsedWalk.PostOpeningDataStages)

            assert (impliedCountSteps = allSteps.Length)

            parsedWalk

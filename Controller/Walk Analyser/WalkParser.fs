
namespace AnalysisOfChangeEngine.Controller.WalkAnalyser


[<RequireQualifiedAccess>]
module WalkParser =

    open FSharp.Reflection
    open FSharp.Quotations
    open AnalysisOfChangeEngine
    open AnalysisOfChangeEngine.StateMonad


    type private WalkState<'TPolicyRecord when 'TPolicyRecord :> IPolicyRecord> =
        Map<SourceElementApiCallDependency<'TPolicyRecord>, Var> * Map<string, SourceElementDefinition<'TPolicyRecord>>
    
    type private SourceParser<'TPolicyRecord, 'TStepResults, 'TApiCollection when 'TPolicyRecord :> IPolicyRecord> =
        SourceExpr<'TPolicyRecord, 'TStepResults, 'TApiCollection>
            -> Stateful<WalkState<'TPolicyRecord>, ParsedSource<'TPolicyRecord>>


    let private getParsedStepSources<'TPolicyRecord, 'TStepResults, 'TApiCollection when 'TPolicyRecord :> IPolicyRecord>
        (stepsPostOpening: IStepHeader list, sourceParser: SourceParser<'TPolicyRecord, 'TStepResults, 'TApiCollection>) =
            // DD - We Process each sourceable step in turn. This includes processing the underlying source expression,
            // and switching out any references to API calls and/or results from the current step with variables.
            // We ensure a one-to-one mapping between each API call and current result reference to a corresponding
            // variable (as mentioned above). The stateful nature of this process allows any such mappings created
            // when processing a given step to be re-used for later steps. This could have been done by passing
            // in a mutable collection. However, this is far from idiomatic and makes such state changes far less
            // explicit.

            // DD - Before we starting using the state monad, we could get away with simpler constructs such as folds.
            // However, the need to be able to re-use the prior step's source for a non-sourceable step
            // means the process of determining a source for each and every step is a tad more cumbersome.
            let sourceableStepHdrs =
                stepsPostOpening
                |> List.choose (function
                    | :? ISourcedStep<'TPolicyRecord, 'TStepResults, 'TApiCollection> as step ->
                        Some (step.Uid, step.Source)
                    | _ ->
                        None)

            // We don't care about the closing state once we've processed all the steps.
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
                :?> ISourcedStep<'TPolicyRecord, 'TStepResults, 'TApiCollection>
                |> _.Uid

            // DD - Doing it this way ensures that the first post-opening step
            // if sourceable!
            let firstPostOpeningParsedStepSource =
                parsedStepMapping[firstPostOpeningStepUid]

            let parsedStepSources =
                stepsPostOpening
                // Ignore the first post-opening step as we've already determined
                // the corresponding parsed source above.
                |> List.tail
                |> List.scan (fun prior step ->
                    match parsedStepMapping.TryFind step.Uid with
                    | Some source -> source
                    // If we can't find a source for this uid, reuse the previous one.
                    | None -> prior) firstPostOpeningParsedStepSource

            assert (stepsPostOpening.Length = parsedStepSources.Length)

            parsedStepSources


    [<RequireQualifiedAccess>]
    module private RemainingBusiness =

        // Used for remaining in-force business.
        let getOpeningDataStage (openingStep, openingDataStageTuples) =
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
    
        // Used for remaining in-force business.
        let getPostOpeningDataStages (postOpeningDataStageTuples: (IStepHeader * _) list) =
            let firstPostOpeningDataChangeStepHeader =
                postOpeningDataStageTuples
                |> List.head
                |> fst
                :?> IDataChangeStep<'TPolicyRecord>

            let dataStageAccumulator accumStage (step: IStepHeader) =
                match step with
                | :? IDataChangeStep<'TPolicyRecord> as step' ->
                    step'
                | _ ->
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
                // We need to make sure step ordering is maintained.
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
                        // from the previous step. As such, we need to be mindful of any
                        // API calls that it makes using that inherited source.
                        withinStageTuples
                        |> List.map snd
                        |> Seq.map _.ApiCalls
                        |> Set.unionMany
                }

            let postOpeningDataStages =
                stepsGroupedByDataStage
                |> List.map toDataStage

            postOpeningDataStages
        

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
                walk.AllSteps
                |> Seq.toList

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

            assert (stepsPostOpening.Length = openingDataStageTuples.Length + postOpeningDataStageTuples.Length)

            let parsedWalk =
                {
                    PostOpeningParsedSteps =
                        postOpeningDataStageTuples
                    RemainingRecordOpeningDataStage =
                        RemainingBusiness.getOpeningDataStage (allSteps[0], openingDataStageTuples)
                    RemainingRecordPostOpeningDataStages =
                        RemainingBusiness.getPostOpeningDataStages postOpeningDataStageTuples
                }

            let impliedCountSteps =
                1 + parsedWalk.RemainingRecordOpeningDataStage.WithinStageSteps.Length
                  + parsedWalk.RemainingRecordPostOpeningDataStages.Length
                  + (parsedWalk.RemainingRecordPostOpeningDataStages |> List.sumBy _.WithinStageSteps.Length)

            assert (impliedCountSteps = allSteps.Length)

            parsedWalk
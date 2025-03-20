
namespace AnalysisOfChangeEngine.Controller.WalkAnalyser


[<RequireQualifiedAccess>]
module WalkParser =

    open FSharp.Quotations
    open AnalysisOfChangeEngine

    
    // DD - A seeming work-around from where the type inferencer was struggling!
    type private SourceParser<'TPolicyRecord, 'TStepResults, 'TApiCollection when 'TPolicyRecord :> IPolicyRecord> =
        Map<string, SourceElementDefinition<'TPolicyRecord>> 
            -> SourceExpr<'TPolicyRecord, 'TStepResults, 'TApiCollection>
            -> ParsedSource<'TPolicyRecord>


    let private getParsedStepSources<'TPolicyRecord, 'TStepResults, 'TApiCollection when 'TPolicyRecord :> IPolicyRecord>
        (stepsPostOpening: IStepHeader list, sourceParser: SourceParser<'TPolicyRecord, 'TStepResults, 'TApiCollection>) =
            let firstParsedStep =
                stepsPostOpening
                |> Seq.head
                |> function
                    | :? ISourcedStep<'TPolicyRecord, 'TStepResults, 'TApiCollection> as step' ->
                        sourceParser Map.empty step'.Source
                    | x ->
                        do printfn "%A" (x.GetType().GetInterfaces())
                        failwith "First step must be a sourceable step."

            let sourceAccumulator priorParsedStep (stepHdr: IStepHeader) =
                match stepHdr with
                | :? ISourcedStep<'TPolicyRecord, 'TStepResults, 'TApiCollection> as step' ->
                    sourceParser priorParsedStep.ElementDefinitions step'.Source

                | _ ->
                    // If we don't have a sourceable step, we just pass the prior mappings along.
                    priorParsedStep

            let parsedStepSources =
                // The opening step cannot have a source.
                stepsPostOpening
                |> List.tail
                |> List.scan sourceAccumulator firstParsedStep

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
            let allSteps =
                walk.AllSteps
                |> Seq.toList

            let stepsPostOpening =
                allSteps
                // We don't care about the opening step here.
                |> List.tail

            let policyRecordVarDef =
                Var ("policyRecord", typeof<'TPolicyRecord>)

            let sourceParser =
                SourceParser.execute<'TPolicyRecord, 'TStepResults, 'TApiCollection>
                    (apiCollection, policyRecordVarDef)

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
                    PolicyRecordVarDef =
                        policyRecordVarDef
                    RemainingRecordOpeningDataStage =
                        getOpeningDataStage (allSteps[0], openingDataStageTuples)
                    RemainingRecordPostOpeningDataStages =
                        getPostOpeningDataStages postOpeningDataStageTuples
                }

            let impliedCountSteps =
                1 + parsedWalk.RemainingRecordOpeningDataStage.WithinStageSteps.Length
                  + parsedWalk.RemainingRecordPostOpeningDataStages.Length
                  + (parsedWalk.RemainingRecordPostOpeningDataStages |> List.sumBy _.WithinStageSteps.Length)

            assert (impliedCountSteps = allSteps.Length)

            parsedWalk
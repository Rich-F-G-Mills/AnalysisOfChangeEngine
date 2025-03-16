
namespace AnalysisOfChangeEngine.Controller.WalkAnalyser


[<RequireQualifiedAccess>]
module WalkParser =

    open FSharp.Quotations
    open FSharp.Reflection
    open AnalysisOfChangeEngine


    let execute (apiCollection: 'TApiCollection) (walk: AbstractWalk<'TPolicyRecord, 'TStepResults, 'TApiCollection>) =
        let stepResultMembers =
            FSharpType.GetRecordFields (typeof<'TStepResults>)
            |> Seq.map (fun pi -> pi.Name, pi)
            |> Map.ofSeq

        let stepsPostOpening =
            walk.AllSteps
            // We don't care about the opening step here.
            |> Seq.tail

        let stepResultMemberNames =
            stepResultMembers
            |> Map.keys
            |> Set.ofSeq

        let policyRecordVarDef =
            Var ("policyRecord", typeof<'TPolicyRecord>)

        let sourceParser =
            SourceParser.execute (apiCollection, policyRecordVarDef)

        let firstParsedStep =
            stepsPostOpening
            |> Seq.head
            |> function
                | :? ISourcedStep<'TPolicyRecord, 'TStepResults, 'TApiCollection> as step' ->
                    sourceParser Map.empty step'.Source
                | _ ->
                    failwith "First step must be a sourceable step."

        let sourceAccumulator priorParsedStep (stepHdr: IStepHeader) =
            match stepHdr with
            | :? ISourcedStep<'TPolicyRecord, 'TStepResults, 'TApiCollection> as step' ->
                sourceParser priorParsedStep.ElementDefinitions step'.Source

            | _ ->
                // If we don't have a sourceable step, we just pass the prior mappings along.
                priorParsedStep

        let parsedStepSources =
            stepsPostOpening
            |> Seq.tail
            |> Seq.scan sourceAccumulator firstParsedStep
            |> Seq.toList

        parsedStepSources

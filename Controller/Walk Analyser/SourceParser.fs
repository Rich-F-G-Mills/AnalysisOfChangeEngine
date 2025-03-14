﻿
namespace AnalysisOfChangeEngine.Controller


[<RequireQualifiedAccess>]
module StepSource =

    open System
    open System.Reflection
    open FSharp.Quotations
    open FSharp.Reflection
    open AnalysisOfChangeEngine
    open AnalysisOfChangeEngine.StateMonad
    open AnalysisOfChangeEngine.Controller.WalkAnalyser


    let private (|SourceLambda|_|) = function
        | Patterns.Lambda (from,
            Patterns.Lambda (policyRecord,
                Patterns.Lambda (priorResults,
                    Patterns.Lambda (currentResults, sourceBody)))) ->
                        Some (from, policyRecord, priorResults, currentResults, sourceBody)
        | _ ->
            None

    // When constructing a record via a code quotation, if the elements are not supplied in
    // the exact same order as the original record type definition, the compiler uses (nested)
    // let bindings which are then passed into the new record expression.
    // This is (likely) just in case the member definitions have side-effects which can then
    // be executed in the same order as the member name-value pairings.
    // Given there are no side-effects to worry about here, we can collapse everything down
    // into the correctly ordered new record expression.
    let private flattenSourceBody (sourceBody: Expr<'TStepResults>) =
        let stepResultMembers =
            FSharpType.GetRecordFields (typeof<'TStepResults>)

        let rec inner mappings expr =
            match expr with
            | Patterns.Let (var, def, body) ->
                inner (mappings |> Map.add var def) body

            | Patterns.NewRecord (recType, values)
                when recType = typeof<'TStepResults> ->
                    let newValues =
                        values
                        |> List.map (function
                            | Patterns.Var v when mappings |> Map.containsKey v ->
                                mappings[v]
                            | expr ->
                                expr)

                    newValues
                    |> Seq.zip stepResultMembers
                    |> Seq.map (fun (pi, v) -> pi.Name, v)
                    |> Map.ofSeq

            | _ ->
                failwith "Unexpected pattern."

        inner Map.empty sourceBody


    let processStep<'TPolicyRecord, 'TStepResults, 'TApiCollection when 'TPolicyRecord :> IPolicyRecord>
        (apiCollection: 'TApiCollection) = 
            let sourceActionType =
                typeof<SourceAction<'TPolicyRecord, 'TStepResults, 'TApiCollection>>

            let dummySourceAction =
                Unchecked.defaultof<SourceAction<'TPolicyRecord, 'TStepResults, 'TApiCollection>>

            let stepResultMembers =
                FSharpType.GetRecordFields (typeof<'TStepResults>)
                |> Seq.map (fun pi -> pi.Name, pi)
                |> Map.ofSeq               

            fun (source: SourceDefinition<'TPolicyRecord, 'TStepResults, 'TApiCollection>) ->
                let fromVarDef, policyRecordsVarDef, priorResultsVarDef, currentResultsVarDef, sourceBody =
                    match source with
                    | SourceLambda (from, policyRecord, priorResults, currentResults, expr) ->
                        (from, policyRecord, priorResults, currentResults, Expr.Cast<'TStepResults> expr)
                    | _ ->
                        failwith "Unexpected source lambda definition."

                let flattenedSourceBody =
                    flattenSourceBody sourceBody

                let (|FromVarDef|_|) = function
                    | v when v = fromVarDef -> Some () | _ -> None

                let (|PolicyRecordVarDef|_|) = function
                    | v when v = policyRecordsVarDef -> Some () | _ -> None

                let (|PriorResultsVarDef|_|) = function
                    | v when v = priorResultsVarDef -> Some () | _ -> None

                let (|CurrentResultsVarDef|_|) = function
                    | v when v = currentResultsVarDef -> Some () | _ -> None

                let (|FromVar|_|) = function
                    | Patterns.Var FromVarDef -> Some () | _ -> None

                let (|PolicyRecordsVar|_|) = function
                    | Patterns.Var PolicyRecordVarDef -> Some () | _ -> None

                let (|PriorResultsVar|_|) = function
                    | Patterns.Var PriorResultsVarDef -> Some () | _ -> None

                let (|CurrentResultsVar|_|) = function
                    | Patterns.Var CurrentResultsVarDef -> Some () | _ -> None

                let (|ApiCallMI|_|) =
                    let callMI =
                        sourceActionType
                            .GetMethod(nameof (dummySourceAction.apiCall))
                            // Unless we get the underlying generic definition, comparisons will always fail.
                            .GetGenericMethodDefinition()

                    fun (mi: MethodInfo) ->
                        let mi' =
                            mi.GetGenericMethodDefinition()

                        if mi' = callMI then Some () else None

                let (|ApiRequest|_|) = function
                    | Patterns.Call (Some FromVar, ApiCallMI, [
                        Patterns.Lambda (_,
                            Patterns.PropertyGet (_, wrappedRequestPI, []))
                        Patterns.Lambda (_,
                            Patterns.PropertyGet (_, selectorPI, []))
                        ]) ->
                            Some (wrappedRequestPI, selectorPI) 
                    | _ ->
                        None

                let (|UsePrior|_|) = function
                    | Patterns.PropertyGet (Some PriorResultsVar, elementPI, []) ->
                        Some elementPI
                    | _ ->
                        None

                let processElementCalculation calcBody =
                    // Why use mutable state via a closure when you can use a state monad?!
                    let rec inner (expr: Expr) =
                        match expr with
                        | ExprShape.ShapeLambda _ ->
                            failwith "Cannot define lambdas within a source element."

                        | Patterns.ValueWithName (_, _, name) ->
                            failwithf "Cannot use closures [%s] within a source element." name

                        | PriorResultsVar ->
                            failwith "Cannot use the prior result in a calculation (ambiguous intent)."

                        | ApiRequest (requestPI, selectorPI) ->
                            state {
                                let apiRequest =
                                    requestPI.GetValue apiCollection
                                    :?> IApiRequestor<'TPolicyRecord>

                                let! varDef =
                                    SourceElementDependencies.addApiCall (apiRequest, selectorPI)                        

                                return Expr.Var varDef
                            }                                    
                
                        | Patterns.PropertyGet (Some CurrentResultsVar, pi, []) ->
                            state {
                                let! varDef =
                                    SourceElementDependencies.addCurrentResult pi

                                return Expr.Var varDef
                            }

                        | ExprShape.ShapeCombination (shape, exprs) ->
                            state {
                                let! newExprs =
                                    List.mapStateM inner exprs

                                return ExprShape.RebuildShapeCombination (shape, newExprs)
                            }

                        | _ ->
                            State.returnM expr                  

                    inner calcBody   
                    |> State.run SourceElementDependencies.empty

                0


(*
[<RequireQualifiedAccess>]
module WalkSourcesParser =

    open FSharp.Reflection
    open AnalysisOfChangeEngine


    let parse (apiCollection: 'TApiCollection) (walk: AbstractWalk<'TPolicyRecord, 'TStepResults, 'TApiCollection>) =
        let stepResultMembers =
            FSharpType.GetRecordFields (typeof<'TStepResults>)
            |> Seq.map (fun pi -> pi.Name, pi)
            |> Map.ofSeq

        let stepsPostOpening =
            walk.AllSteps
            // We don't care about the opening step here.
            |> Seq.tail

        let startingParsedSource =
            stepsPostOpening
            |> Seq.head
            // The fist post-opening step MUST have a source; otherwise, what are we using?
            :?> IApiSourcedStep<'TPolicyRecord, 'TStepResults, 'TApiCollection>
            |> _.Source
            |> StepSourceParser.parse apiCollection

        let stepResultMemberNames =
            stepResultMembers
            |> Map.keys
            |> Set.ofSeq

        let startingParsedSourceNames =
            startingParsedSource
            |> Map.keys
            |> Set.ofSeq

        if stepResultMemberNames <> startingParsedSourceNames then
            failwith "Incorrect mappings supplied for inital post-opening step."

        let accumulator prior: IStepHeader -> Map<string, StepSourceParser.SourceElement<_, _>> = function
            | :? IApiSourcedStep<'TPolicyRecord, 'TStepResults, 'TApiCollection> as step' ->
                let parsedSource =
                    step'.Source
                    |> StepSourceParser.parse apiCollection

                let parsedSourceKeys =
                    parsedSource
                    |> Map.keys
                    |> Set.ofSeq

                if not (Set.isSubset parsedSourceKeys stepResultMemberNames) then
                    failwithf "Unexpected result mappings supplied for step '%s'." step'.Title

                let accumParsedSource =
                    parsedSource
                    |> Map.merge prior

                let calculatedElements =
                    accumParsedSource
                    |> Map.toSeq
                    |> Seq.choose (function
                        | memberName, { Type = StepSourceParser.SourceElementType.Calculation (dependsOn, _) } ->
                            Some (memberName, (set dependsOn.Keys))
                        | _ ->
                            None)
                    |> Map.ofSeq

                let nonCalculatedElements =
                    calculatedElements
                    |> Map.keys
                    |> Set
                    |> Set.difference stepResultMemberNames

                accumParsedSource

            | _ ->
                // If we don't have a sourceable step, we just pass the prior mappings along.
                prior

        stepsPostOpening
        // Drop the first of the post-opening steps.
        |> Seq.tail
        |> Seq.scan accumulator startingParsedSource
        |> Seq.toList
*)
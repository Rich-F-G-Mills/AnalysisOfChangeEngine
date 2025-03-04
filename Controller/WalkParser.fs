
namespace AnalysisOfChangeEngine.Controller


[<RequireQualifiedAccess>]
module StepSourceParser =

    open System.Reflection
    open FSharp.Quotations
    open FSharp.Reflection
    open AnalysisOfChangeEngine


    [<RequireQualifiedAccess>]
    type SourceElementType<'TPolicyRecord when 'TPolicyRecord :> IPolicyRecord> =
        | ApiCall of Requestor: Map<string, ApiRequest<'TPolicyRecord>> * Output: PropertyInfo
        | Calculation of DependsOn: Map<string, PropertyInfo> * OriginalDefinition: Expr * RebuiltDefinition: Expr

    [<NoEquality; NoComparison>]
    type SourceElement<'TPolicyRecord, 'TStepResults when 'TPolicyRecord :> IPolicyRecord> =
        {
            PropertyInfo    : PropertyInfo
            Type            : SourceElementType<'TPolicyRecord>
        }

    type ParsedSource<'TPolicyRecord, 'TStepResults when 'TPolicyRecord :> IPolicyRecord> =
        Map<string, SourceElement<'TPolicyRecord, 'TStepResults>>


    let private (|SourceLambda|_|) = function
        | Patterns.Lambda (from,
            Patterns.Lambda (prior, expr)) ->
                Some (from, prior, expr)
        | _ ->
            None

    // When constructing a record via a code quotation, if the elements are not supplied in
    // the exact same order as the original record type definition, the compiler uses (nested)
    // let bindings which are then passed into the new record expression.
    // This is (likely) just in case the member definitions have side-effects which can then
    // be executed in the same order as the member name-value pairings.
    // Given there are no side-effects to worry about here, we can collapse everything down
    // into the correctly ordered new record expression.
    let private getSourceBodyElements (sourceBody: Expr<'TStepResults>) =
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


    let parse<'TPolicyRecord, 'TStepResults, 'TApiCollection when 'TPolicyRecord :> IPolicyRecord>
        (apiCollection: 'TApiCollection) = 
            let sourceActionType =
                typeof<SourceAction<'TPolicyRecord, 'TStepResults, 'TApiCollection>>

            let dummySourceAction =
                Unchecked.defaultof<SourceAction<'TPolicyRecord, 'TStepResults, 'TApiCollection>>

            let stepResultMembers =
                FSharpType.GetRecordFields (typeof<'TStepResults>)
                |> Seq.map (fun pi -> pi.Name, pi)
                |> Map.ofSeq

            let getCalculationDependencies (resultsVar, calcBody: Expr) =
                let rec inner = function
                    | Patterns.PropertyGet (Some (Patterns.Var v), pi, [])
                        when v = resultsVar ->
                            Set.singleton pi.Name
                    | ExprShape.ShapeCombination (_, exprs) ->
                        exprs
                        |> Seq.map inner
                        |> Set.unionMany
                    | _ ->
                        Set.empty

                let dependsOnMembers =
                    inner calcBody

                stepResultMembers
                |> Map.filter (fun name _ -> dependsOnMembers.Contains name)
                

            fun (source: SourceDefinition<'TPolicyRecord, 'TStepResults, 'TApiCollection>) ->
                let fromVarDef, priorVarDef, sourceBody =
                    match source with
                    | SourceLambda (from, prior, expr) ->
                        (from, prior, expr)
                    | _ ->
                        failwith "Unexpected source lambda definition."

                let sourceBody =
                    Expr.Cast<'TStepResults> sourceBody

                let (|FromVarDef|_|) = function
                    | v when v = fromVarDef -> Some () | _ -> None

                let (|PriorVarDef|_|) = function
                    | v when v = priorVarDef -> Some () | _ -> None

                let (|FromVar|_|) = function
                    | Patterns.Var FromVarDef -> Some () | _ -> None

                let (|PriorVar|_|) = function
                    | Patterns.Var PriorVarDef -> Some () | _ -> None

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

                let (|CalculationMI|_|) =
                    let callMI =
                        sourceActionType
                            .GetMethod(nameof (dummySourceAction.calculation))
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
                 
                let (|Calculation|_|) = function
                    | Patterns.Call (Some FromVar, CalculationMI, [
                        Patterns.Lambda (resultsVar, calcBody)]) ->
                        Some (resultsVar, calcBody)
                    | _ ->
                        None

                let (|UsePrior|_|) = function
                    | Patterns.PropertyGet (Some PriorVar, elementPI, []) ->
                        Some elementPI
                    | _ ->
                        None
                               
                               
                let parseSourceElementType (sourceElementPI: PropertyInfo) = function
                    | ApiRequest (wrappedRequestPI, selectorPI)
                        // In theory, a mismatch here shouldn't be possible, but it's better to be safe!
                        when sourceElementPI.PropertyType = selectorPI.PropertyType  ->
                            let (wrappedRequest: IApiRequestor<'TPolicyRecord>) =
                                downcast wrappedRequestPI.GetValue apiCollection

                            let sourceElementType =
                                SourceElementType.ApiCall (
                                    Map.singleton wrappedRequest.Name wrappedRequest.Requestor,
                                    selectorPI
                                )

                            Some sourceElementType

                    | Calculation (resultsVar, calcBody)
                        when sourceElementPI.PropertyType = calcBody.Type ->                  
                            let dependsOn =
                                getCalculationDependencies (resultsVar, calcBody)                        

                            let sourceElementType =
                                SourceElementType.Calculation (dependsOn, calcBody, calcBody)

                            Some sourceElementType

                    | UsePrior priorSourceElementPI when priorSourceElementPI = sourceElementPI ->
                        None

                    | pattern ->
                        failwithf "Unexpected pattern: %A" pattern

                let parsedSourceElements: Map<string, SourceElement<'TPolicyRecord, 'TStepResults>> =
                    match sourceBody with
                    | PriorVar ->
                        Map.empty

                    | sourceBody ->
                        let sourceElements =
                            getSourceBodyElements sourceBody

                        stepResultMembers
                        |> Map.keys
                        |> Seq.map (fun name ->
                            name, stepResultMembers[name], sourceElements[name])
                        |> Seq.map (fun (name, pi, defn) ->
                            name, pi, parseSourceElementType pi defn)
                        |> Seq.choose (function
                            // Ignore any source elements where we're just using the prior definition. 
                            | name, pi, Some elementType ->
                                Some (name, { PropertyInfo = pi; Type = elementType })
                            | _ ->
                                None)
                        |> Map.ofSeq
                    
                parsedSourceElements



[<RequireQualifiedAccess>]
module WalkParser =

    open FSharp.Reflection
    open AnalysisOfChangeEngine


    let parseStepSourcesForWalk (apiCollection: 'TApiCollection) (walk: AbstractWalk<'TPolicyRecord, 'TStepResults, 'TApiCollection>) =
        let stepResultMembers =
            FSharpType.GetRecordFields (typeof<'TStepResults>)

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

        let stepResultMemberKeys =
            stepResultMembers
            |> Seq.map _.Name
            |> Set.ofSeq

        let startingParsedSourceKeys =
            startingParsedSource
            |> Map.keys
            |> Set.ofSeq

        if stepResultMemberKeys <> startingParsedSourceKeys then
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

                if not (Set.isSubset parsedSourceKeys stepResultMemberKeys) then
                    failwithf "Unexpected result mappings supplied for step '%s'." step'.Title

                parsedSource
                |> Map.merge prior

            | _ ->
                // If we don't have a sourceable step, we just pass the prior mappings along.
                prior

        stepsPostOpening
        // Drop the first of the post-opening steps.
        |> Seq.tail
        |> Seq.scan accumulator startingParsedSource
        |> Seq.toList
                
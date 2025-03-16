
namespace AnalysisOfChangeEngine.Controller.WalkAnalyser


[<RequireQualifiedAccess>]
module SourceParser =

    open System
    open System.Reflection
    open FSharp.Quotations
    open FSharp.Reflection
    open AnalysisOfChangeEngine
    open AnalysisOfChangeEngine.StateMonad


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


    let execute<'TPolicyRecord, 'TStepResults, 'TApiCollection when 'TPolicyRecord :> IPolicyRecord>
        (apiCollection: 'TApiCollection, newPolicyRecordVarDef) = 
            let sourceActionType =
                typeof<SourceAction<'TPolicyRecord, 'TStepResults, 'TApiCollection>>

            let dummySourceAction =
                Unchecked.defaultof<SourceAction<'TPolicyRecord, 'TStepResults, 'TApiCollection>>

            let stepResultMembers =
                FSharpType.GetRecordFields (typeof<'TStepResults>)
                |> Seq.map (fun pi -> pi.Name, pi)
                |> Map.ofSeq   
                
            let stepResultMemberNames =
                stepResultMembers
                |> Map.keys
                |> Set

            fun priorElementDefns (source: SourceExpr<'TPolicyRecord, 'TStepResults, 'TApiCollection>) ->
                let fromVarDef, policyRecordVarDef, priorResultsVarDef, currentResultsVarDef, sourceBody =
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
                    | v when v = policyRecordVarDef -> Some () | _ -> None

                let (|PriorResultsVarDef|_|) = function
                    | v when v = priorResultsVarDef -> Some () | _ -> None

                let (|CurrentResultsVarDef|_|) = function
                    | v when v = currentResultsVarDef -> Some () | _ -> None

                let (|FromVar|_|) = function
                    | Patterns.Var FromVarDef -> Some () | _ -> None

                let (|PolicyRecordVar|_|) = function
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

                let processElementCalculation calcDefn: SourceElementDefinition<'TPolicyRecord> =
                    // Why use mutable state via a closure when you can use a state monad?!
                    // It's obvious... Right???
                    let rec inner = function
                        | Patterns.Lambda _ ->
                            failwith "Cannot define lambdas within a source element."

                        | Patterns.ValueWithName (_, _, name) ->
                            failwithf "Cannot use closures [%s] within a source element." name

                        | PriorResultsVar ->
                            failwith "Cannot use the prior result in a calculation (ambiguous intent)."

                        | PolicyRecordVar ->
                            // Allows us to standardise the policy record variable across all sources.
                            State.returnM (Expr.Var newPolicyRecordVarDef)

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

                        | Patterns.Call (Some obj, mi, exprs) ->                            
                            state {
                                let! newExprs =
                                    List.mapStateM inner exprs

                                return Expr.Call (obj, mi, newExprs)
                            }

                        | Patterns.Call (None, mi, exprs) ->                            
                            state {
                                let! newExprs =
                                    List.mapStateM inner exprs

                                return Expr.Call (mi, newExprs)
                            }

                        | Patterns.Value _ as valueExpr ->
                            State.returnM valueExpr

                        | expr ->
                            failwithf "Unsupported expression: %A" expr

                    let newCalcDefn, dependencies =
                        State.run SourceElementDependencies.empty (inner calcDefn)

                    {
                        Dependencies    = dependencies
                        Original        = calcDefn
                        Rebuilt         = newCalcDefn
                    }

                let processElement elementPI = function
                    | UsePrior elementPI' when elementPI = elementPI' ->
                        None

                    | calcDefn ->
                        Some (processElementCalculation calcDefn)

                let definedElements =
                    flattenedSourceBody
                    |> Map.toSeq
                    |> Seq.map (fun (name, defn) ->
                        name, processElement stepResultMembers[name] defn)
                    |> Seq.choose (function
                        | name, Some element -> Some (name, element)
                        | _ -> None)
                    |> Map.ofSeq

                let combinedElements =
                    Map.merge priorElementDefns definedElements

                let combinedElementNames =
                    combinedElements
                    |> Map.keys
                    |> Set

                // Provided the first post-opening step has been defined correctly,
                // we shouldn't have any issues thereafter.
                if combinedElementNames <> stepResultMemberNames then
                    failwith "Not all step result members have been defined."

                // Here we're identifying dependencies between elements of the
                // current step.
                let withinStepDependencies =
                    combinedElements
                    |> Map.map (fun _ { Dependencies = deps } ->
                        deps.CurrentResults
                        |> Map.keys
                        |> Seq.map _.ElementProperty.Name
                        |> Set)

                let dependencySorter ordered =
                    // Not the most efficient way of doing things.
                    // However, a set doesn't maintain insertion order.
                    // And, frankly, far from being a performance bottleneck.
                    let orderedSet =
                        Set ordered

                    let remaining =
                        Set.difference stepResultMemberNames orderedSet

                    if remaining.IsEmpty then
                        // Nothing else left to sort!
                        None

                    else
                        let next =
                            remaining
                            |> Seq.tryFind (fun elementName ->
                                let dependsOn =
                                    withinStepDependencies[elementName]
                                Set.isSubset dependsOn orderedSet)
                            |> Option.defaultWith (fun _ ->
                                failwith "Circular dependency detected.")

                        let newOrdered =
                            ordered @ [next]

                        Some (next, newOrdered)

                let memberOrdering =
                    List.unfold dependencySorter List.empty          

                {
                    ElementDefinitions  = combinedElements
                    Ordering            = memberOrdering
                }
                            
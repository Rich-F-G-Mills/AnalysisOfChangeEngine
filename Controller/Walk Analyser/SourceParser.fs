
// This deals with parsing the source for a given step. Note that
// only certain steps will (/can) have a source specified.
// Once the source for a step has been processed, we will:
//  * Have rebuilt the logic to obtain the corresponding member value.
//  * Have identified all of the dependencies, both API and internal.
//  * Have identified the required calculation ordering of result members.

namespace AnalysisOfChangeEngine.Controller.WalkAnalyser


[<RequireQualifiedAccess>]
module internal SourceParser =

    open System.Reflection
    open FSharp.Quotations
    open FSharp.Reflection
    open AnalysisOfChangeEngine
    open AnalysisOfChangeEngine.Common
    open AnalysisOfChangeEngine.Common.StateMonad

    (*
        When constructing a record via a code quotation, if the elements are not supplied in
        the exact same order as the original record type definition, the compiler uses (nested)
        let bindings which are then passed into the new record expression.
        This is (likely) just in case the member definitions have side-effects which can then
        be executed in the same order in which the member name-value pairings are supplied.
        Given there are no side-effects to worry about here, we can collapse everything down
        into the correctly ordered new record expression.
    *)
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

    let private dependencySorter (stepResultMemberNames: string Set, withinStepDependencies: Map<_, _>) ordered =
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
                // Only using tryFind so we can return a (marginally) more helpful error.
                |> Seq.tryFind (fun elementName ->
                    let dependsOn =
                        withinStepDependencies[elementName]
                    Set.isSubset dependsOn orderedSet)
                |> Option.defaultWith (fun _ ->
                    failwith "Circular dependency detected.")

            let newOrdered =
                // Would be better to prepend and then reverse. However, not going
                // to be an issue given the list sizes here.
                ordered @ [next]

            Some (next, newOrdered)


    let internal execute<'TPolicyRecord, 'TStepResults, 'TApiCollection>
        (apiCollection: 'TApiCollection, newPolicyRecordVarDef, currentResultsVarDefMapping) = 
            let sourceActionType =
                typeof<SourceAction<'TPolicyRecord, 'TStepResults, 'TApiCollection>>

            let dummySourceAction =
                Unchecked.defaultof<SourceAction<'TPolicyRecord, 'TStepResults, 'TApiCollection>>

            let stepResultsType =
                typeof<'TStepResults>

            let stepResultMembers =
                FSharpType.GetRecordFields (stepResultsType)
                |> Seq.map (fun pi -> pi.Name, pi)
                |> Map.ofSeq   
                
            let stepResultMemberNames =
                stepResultMembers
                |> Map.keys
                |> Seq.toList

            let stepResultMemberNamesSet =
                Set stepResultMemberNames

            let sourceInvokerFactory =
                SourceInvoker.create<'TPolicyRecord, 'TStepResults>
                    (newPolicyRecordVarDef, currentResultsVarDefMapping)

            // When specificying the source for a given step, we can inherite
            // element definitions from the previous step. As such, we need
            // to see what they were so we can merge with the current step's source.
            // Given we're inheriting prior elements, we also need to maintain
            // mappings for all API calls that we've encountered.
            fun (source: SourceExpr<'TPolicyRecord, 'TStepResults, 'TApiCollection>) ->
                stateful {
                    let! (apiCallVarDefMapping, accruedElements) =
                        Stateful.get

                    let fromVarDef, policyRecordVarDef, priorResultsVarDef, currentResultsVarDef, sourceBody =
                        match source with
                        | SourceExpr.Definition (fromVarDef', policyRecordVarDef', priorResultsVarDef', currentResultsVarDef', sourceBody') ->
                            (fromVarDef', policyRecordVarDef', priorResultsVarDef', currentResultsVarDef', Expr.Cast<'TStepResults> sourceBody')
                        | _ ->
                            failwith "Unexpected source lambda definition."

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

                            // Make sure we're comparing generic vs generic MIs.
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

                    let processElementCalculation (elementPI: PropertyInfo, calcDefn) =
                        stateful {
                            // Why use mutable state via a closure when you can use a state monad?!
                            let rec inner = function
                                | Patterns.Lambda _ ->
                                    failwith "Cannot define lambdas within a source element."

                                | Patterns.ValueWithName (_, _, name) ->
                                    failwithf "Cannot use closures [%s] within a source element." name

                                | PriorResultsVar ->
                                    failwith "Cannot use the prior result in a calculation (ambiguous intent)."

                                | PolicyRecordVar ->
                                    // Allows us to standardise the policy record variable across all sources.
                                    Stateful.returnM (Expr.Var newPolicyRecordVarDef)

                                | ApiRequest (requestPI, selectorPI) ->
                                    stateful {
                                        let (wrappedApiRequest: IWrappedApiRequestor<'TPolicyRecord>) =
                                            downcast requestPI.GetValue apiCollection                                           

                                        let! varDef =
                                            SourceElementDependencies.registerApiCall
                                                (wrappedApiRequest.UnderlyingRequestor, selectorPI)                        

                                        return Expr.Var varDef
                                    }                                    
                
                                | Patterns.PropertyGet (Some CurrentResultsVar, pi, []) ->
                                    stateful {
                                        do! SourceElementDependencies.registerCurrentResult pi.Name

                                        return Expr.Var (Map.find pi.Name currentResultsVarDefMapping)
                                    }

                                // DD - Previously we were using ExprShape.ShapeCombination.
                                // ...However, this was leading to unanticipated items being returned
                                // within 'exprs'. Have decided to use specific patterns instead
                                // so as to better control what expression elements can get used.
                                | Patterns.Call (Some obj, mi, exprs) ->                            
                                    stateful {
                                        let! newExprs =
                                            List.mapStateM inner exprs

                                        return Expr.Call (obj, mi, newExprs)
                                    }

                                | Patterns.Call (None, mi, exprs) ->                            
                                    stateful {
                                        let! newExprs =
                                            List.mapStateM inner exprs

                                        return Expr.Call (mi, newExprs)
                                    }

                                | Patterns.Value _ as valueExpr ->
                                    // I know, I know... Could just use Stateful.returnM!
                                    stateful {
                                        return valueExpr
                                    }

                                | expr ->
                                    failwithf "Unsupported expression: %A" expr                            

                            let! apiCallVarDefMapping' =
                                Stateful.get
                            
                            let newCalcBody, (newApiCallVarDefMapping, elementDependencies) =
                                Stateful.run
                                    (apiCallVarDefMapping', SourceElementDependencies.empty)
                                    (inner calcDefn)

                            do! Stateful.put newApiCallVarDefMapping

                            return {
                                Dependencies            = elementDependencies
                                OriginalExprBody        = calcDefn
                                RebuiltExprBody         = newCalcBody
                            }
                        }                        

                    let specifiedElementExprs =
                        sourceBody
                        |> flattenSourceBody 
                        |> Map.toSeq
                        |> Seq.filter (function
                            // Remove those elements which simply refer to that same element
                            // from the previous step.
                            | name, UsePrior elementPI' -> stepResultMembers[name] <> elementPI'
                            | _ -> true)
                        |> Seq.toList

                    let (specifiedElements, newApiCallVarDefMapping) =
                        specifiedElementExprs
                        |> List.mapStateM (fun (name, expr) ->
                            processElementCalculation (stepResultMembers[name], expr)
                            |> Stateful.mapM (fun defn -> name, defn))
                        |> Stateful.mapM Map.ofList
                        |> Stateful.run apiCallVarDefMapping                  
                        
                    let combinedElements =
                        Map.merge accruedElements specifiedElements

                    let combinedElementNamesSet =
                        combinedElements
                        |> Map.keys
                        |> Set

                    // In theory, given the first sourceable step (ie. opening re-run) does not
                    // allow the user to inherit prior elements, this should never happen!
                    if combinedElementNamesSet <> stepResultMemberNamesSet then
                        failwith "Not all step result members have been defined."

                    // Update our state tracking both API dependencies and
                    // the accrued set of element definitions.
                    do! Stateful.put (newApiCallVarDefMapping, combinedElements)

                    // Here we're identifying dependencies between elements of the
                    // current step.
                    let withinStepDependencies =
                        combinedElements
                        |> Map.map (fun _ ->
                            _.Dependencies.CurrentResults) 
                        
                    let dependencySorter' =
                        dependencySorter (stepResultMemberNamesSet, withinStepDependencies)

                    let elementOrdering =
                        List.unfold dependencySorter' List.empty     
                    
                    let invokerDetails =
                        sourceInvokerFactory (newApiCallVarDefMapping, combinedElements, elementOrdering)

                    return {
                        ElementDefinitions  = combinedElements
                        //ApiCallsTupleType   = invokerDetails.ApiCallsTupleType
                        ApiCalls            = invokerDetails.CombinedApiCalls
                        RebuiltSourceExpr   = invokerDetails.RebuiltSourceExpr
                        Invoker             = invokerDetails.WrappedInvoker
                    }
                }
                            

namespace AnalysisOfChangeEngine.Controller.WalkAnalyser


module internal SourceInvoker =

    open FSharp.Linq.RuntimeHelpers
    open FSharp.Quotations
    open FSharp.Reflection


    let private unitObj: obj =
        upcast ()

    // Note that, on a number of occassions we are iterating over sets.
    // The documentation states that elements within F# sets are ordered
    // according to their value, not insertion order.
    let internal create<'TPolicyRecord, 'TStepResults>
        (policyRecordVarDef: Var, currentResultsVarDefMapping: Map<_, Var>) =
            let policyRecordType =
                typeof<'TPolicyRecord>

            let stepResultsType =
                typeof<'TStepResults>
            
            // When we construct the step record, we need to supply values
            // in the same order as the fields in this array.
            let stepResultMembers =
                FSharpType.GetRecordFields (stepResultsType) 

            (*
            Ultimately, the step results record will be defined as:
                fun policyRecord (apiCall1, apiCall2, ...) ->
                    let `ApiCall.<SomeRequestor>.<UAS>` = apiCall1 
                    let `ApiCall.<SomeRequestor>.<SAS>` = apiCall2
                    ...
                    let `CurrentResult.<UAS>` = ... `ApiCall.<SomeRequestor>.<UAS>` ...
                    let `CurrentResult.<SAS>` = ... `ApiCall.<SomeRequestor>.<SAS>` ...
                    ...
                        {
                            UAS = `CurrentResult.<UAS>`
                            SAS = `CurrentResult.<SAS>`
                            ...
                        }

            This allows us to more easily handle situations where step elements
            depend on other elements within that same step. If (bizarely) it turned
            out the UAS depended on SAS, the ordering of the let assignments would change:
                ...                
                let `CurrentResult.<SAS>` = ...
                let `CurrentResult.<UAS>` = ... `CurrentResult.<SAS>` ...
                ...
            *)
            let newRecordExpr =
                Expr.NewRecord(
                    stepResultsType,
                    // Need to use the list of member names as ordering is critical!
                    stepResultMembers
                    |> Seq.map (fun pi ->
                        Expr.Var currentResultsVarDefMapping[pi.Name])
                    |> Seq.toList
                )

            fun (apiCallVarDefMapping, combinedElements: Map<_, SourceElementDefinition<'TPolicyRecord>>, elementOrdering) ->         
                assert (policyRecordType = policyRecordVarDef.Type)

                // Combine the API calls for all element definitions within this step
                // into a Set. Ensures that all API dependencies will be unique.
                let combinedApiCalls =
                    combinedElements
                    |> Map.values
                    |> Seq.map _.Dependencies.ApiCalls
                    |> Set.unionMany

                let apiCallsTypes =
                    combinedApiCalls
                    |> Seq.map _.OutputProperty.PropertyType
                    |> Seq.toArray

                // This is the type of our argument that will receive all API output
                // values for this specific step.
                let apiCallsTupleType =
                    FSharpType.MakeTupleType apiCallsTypes

                let apiCallsTupleVarDef =
                    Var ("apiCalls", apiCallsTupleType)

                let apiCallsTupleVar =
                    Expr.Var apiCallsTupleVarDef

                // For each API output, map this to the corresponding variable.
                // Note that any given output for the same API requestor, will
                // use the same variable throughout the walk.
                let apiCallsVarDefs =
                    combinedApiCalls
                    |> Seq.map (fun apiCall ->
                        Map.find apiCall apiCallVarDefMapping)
                    |> Seq.indexed  
                    |> Seq.toList

                let elementAssignments =
                    List.foldBack (fun name inner ->
                        Expr.Let (
                            currentResultsVarDefMapping[name],
                            combinedElements[name].RebuiltExprBody,
                            inner
                        )) elementOrdering newRecordExpr

                let rebuiltSourceExprBody =
                    apiCallsVarDefs
                    |> List.fold (fun inner (idx, var) ->
                        Expr.Let (var, Expr.TupleGet (apiCallsTupleVar, idx), inner)) elementAssignments

                let rebuiltSourceExpr =
                    Expr.Lambda(policyRecordVarDef,
                        Expr.Lambda (apiCallsTupleVarDef,
                            rebuiltSourceExprBody))

                let invoker =
                    LeafExpressionConverter.EvaluateQuotation rebuiltSourceExpr

                let fsharpFuncType =
                    typedefof<OptimizedClosures.FSharpFunc<_, _, _>>
                        .MakeGenericType(
                            policyRecordType,
                            apiCallsTupleType,
                            stepResultsType
                        )

                let fsharpFunc =
                    // Note that Adapt is a static method.
                    fsharpFuncType.GetMethod("Adapt").Invoke(null, [|invoker|])

                let invokerArgTypes =
                    [| policyRecordType; apiCallsTupleType |]

                let invokerMI =                
                    fsharpFuncType.GetMethod("Invoke", invokerArgTypes)

                let wrappedInvoker (policyRecord: 'TPolicyRecord, apiCallResults: obj array)
                    : 'TStepResults =
                        let apiCallResults' =
                            if apiCallsTupleType = typeof<unit> then
                                unitObj
                            else
                                FSharpValue.MakeTuple (apiCallResults, apiCallsTupleType)

                        downcast invokerMI.Invoke
                            (fsharpFunc, [| policyRecord; apiCallResults' |])

                {|
                    //ApiCallsTupleType       = apiCallsTupleType
                    CombinedApiCalls        = combinedApiCalls
                    RebuiltSourceExpr       = rebuiltSourceExpr
                    WrappedInvoker          = wrappedInvoker
                |}

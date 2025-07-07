
namespace AnalysisOfChangeEngine.Controller.WalkAnalyser


module internal SourceInvoker =

    open FSharp.Linq.RuntimeHelpers
    open FSharp.Quotations
    open FSharp.Reflection


    let private unitObj =
        () :> obj

    // Note that, on a number of occassions we are iterating over sets.
    // The documentation states that elements within F# sets are ordered
    // according to their value, not insertion order.
    let internal create<'TPolicyRecord, 'TStepResults>
        (policyRecordVarDef: Var, currentResultsVarDefMapping: Map<_, Var>) =
            let policyRecordType =
                typeof<'TPolicyRecord>

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

            let newRecordExpr =
                Expr.NewRecord(
                    stepResultsType,
                    // Need to use the list of member names as ordering is critical!
                    stepResultMemberNames
                    |> Seq.map (fun name ->
                        Expr.Var currentResultsVarDefMapping[name])
                    |> Seq.toList
                )

            fun (apiCallVarDefMapping, combinedElements: Map<_, SourceElementDefinition<'TPolicyRecord>>, elementOrdering) ->         
                assert (policyRecordType = policyRecordVarDef.Type)

                let combinedApiCalls =
                    combinedElements
                    |> Map.values
                    |> Seq.map _.Dependencies.ApiCalls
                    |> Set.unionMany

                let apiCallsTypes =
                    combinedApiCalls
                    |> Seq.map _.OutputProperty.PropertyType
                    |> Seq.toArray

                let apiCallsTupleType =
                    FSharpType.MakeTupleType apiCallsTypes

                let apiCallsTupleVarDef =
                    Var ("apiCalls", apiCallsTupleType)

                let apiCallsTupleVar =
                    Expr.Var apiCallsTupleVarDef

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
                    ApiCallsTupleType       = apiCallsTupleType
                    CombinedApiCalls        = combinedApiCalls
                    RebuiltSourceExpr       = rebuiltSourceExpr
                    WrappedInvoker          = wrappedInvoker
                |}

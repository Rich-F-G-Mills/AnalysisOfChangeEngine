



namespace AnalysisOfChangeEngine.Controller.WalkAnalyser


module ElementInvoker =

    open System.Reflection
    open FSharp.Linq.RuntimeHelpers
    open FSharp.Quotations
    open FSharp.Reflection
    open AnalysisOfChangeEngine


    let private unitObj =
        () :> obj

    let internal create<'TPolicyRecord>
        (* Design Decision:
            We expect that the first set of tupled arguments will be applied first (and once).
            The remaining parameters will then be supplied for each element in all steps.
        *)
        (policyRecordVarDef: Var, currentResultsVarDefMapping: Map<_, Var>)
        (apiCallVarDefMapping, elementPI: PropertyInfo, newCalcBody: Expr, dependencies: SourceElementDependencies<'TPolicyRecord>) =
            let policyRecordType =
                typeof<'TPolicyRecord>

            assert (policyRecordType = policyRecordVarDef.Type)
            assert (elementPI.PropertyType = newCalcBody.Type)

            let apiCallsTupleType =
                dependencies.ApiCalls
                |> Seq.map _.OutputProperty.PropertyType
                |> Seq.toArray
                |> function
                    | [||] -> typeof<unit>
                    | ts -> FSharpType.MakeTupleType ts

            let apiCallsTupleVarDef =
                Var ("apiCalls", apiCallsTupleType)

            let apiCallsTupleVar =
                Expr.Var apiCallsTupleVarDef

            let apiCallsVarDefs =
                dependencies.ApiCalls
                |> Seq.map (fun apiCall ->
                    // There's no tryFind here. Failure is NOT an option.
                    Map.find apiCall apiCallVarDefMapping)
                |> Seq.indexed

            let currentResultsTupleType =
                dependencies.CurrentResults
                |> Seq.map (fun name ->
                    currentResultsVarDefMapping[name].Type)
                |> Seq.toArray
                |> function
                    | [||] -> typeof<unit>
                    | ts -> FSharpType.MakeTupleType ts

            let currentResultsTupleVarDef =
                Var ("currentResults", currentResultsTupleType)

            let currentResultsTupleVar =
                Expr.Var currentResultsTupleVarDef

            let currentResultsVarDefs =
                dependencies.CurrentResults
                |> Seq.map (fun name ->
                    Map.find name currentResultsVarDefMapping)
                |> Seq.indexed
                        
            let apiCallsAssignments =
                apiCallsVarDefs
                |> Seq.fold (fun inner (idx, var) ->
                    Expr.Let (var, Expr.TupleGet (apiCallsTupleVar, idx), inner)) newCalcBody

            let rebuiltCalcExprBody =
                currentResultsVarDefs
                |> Seq.fold (fun inner (idx, var) ->
                    Expr.Let (var, Expr.TupleGet (currentResultsTupleVar, idx), inner)) apiCallsAssignments

            let rebuiltCalcExpr =
                Expr.Lambda (policyRecordVarDef,
                    Expr.Lambda (apiCallsTupleVarDef,
                        Expr.Lambda (currentResultsTupleVarDef,
                            rebuiltCalcExprBody)))

            let invoker =
                LeafExpressionConverter.EvaluateQuotation rebuiltCalcExpr

            // We now have some ceremony where we're trying to create
            // a wrapper by which to invoke our compiled lambda above.
            let fsharpFuncType =
                typedefof<OptimizedClosures.FSharpFunc<_, _, _, _>>
                    .MakeGenericType(
                        policyRecordType,
                        apiCallsTupleType,
                        currentResultsTupleType,
                        elementPI.PropertyType
                    )

            let fsharpFunc =
                // Converts our compiled lambda into an FSharpFunc that we can 'fast' invoke.
                fsharpFuncType.GetMethod("Adapt").Invoke(null, [|invoker|])

            let invokerArgTypes =
                [| policyRecordType; apiCallsTupleType; currentResultsTupleType |]

            let invokerMI =                
                fsharpFuncType.GetMethod("Invoke", invokerArgTypes)

            // Provider a wrapped invoker of our element calculation logic.
            // Only intended that this will be used for testing purposes.
            let wrappedInvoker (policyRecord: 'TPolicyRecord, apiCallResults, currentResults) =
                let apiCallResults' =
                    if apiCallsTupleType = typeof<unit> then
                        unitObj
                    else
                        FSharpValue.MakeTuple (apiCallResults, apiCallsTupleType)

                let currentResults' =
                    if currentResultsTupleType = typeof<unit> then
                        unitObj
                    else
                        FSharpValue.MakeTuple (currentResults, currentResultsTupleType)

                invokerMI.Invoke
                    (fsharpFunc, [| policyRecord; apiCallResults'; currentResults' |])

            {|
                ApiCallsTupleType       = apiCallsTupleType
                CurrentResultsTupleType = currentResultsTupleType
                WrappedInvoker          = wrappedInvoker
            |}

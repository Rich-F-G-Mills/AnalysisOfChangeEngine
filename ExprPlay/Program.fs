
module Program =

    open FSharp.Quotations
    open FSharp.Reflection
    open FSharp.Linq.RuntimeHelpers
    open FsToolkit.ErrorHandling

    let getArray (arr: Expr) idx =
        <@@ (%%arr: string array)[idx] @@>

    let createInstantiator<'TTuple> =
        let tupleLength =
            FSharpType.GetTupleElements(typeof<'TTuple>).Length

        let reqdArrayLenVar =
            Var ("reqdArrayLen", typeof<int>)

        let reqdArrayLenVarExpr =
            Expr.Var reqdArrayLenVar

        let tupleResultVar =
            Var ("tupleResult", typeof<'TTuple>)

        let tupleResultVarExpr =
            Expr.Var tupleResultVar

        let underlyingExpr: Expr<string array -> Result<'TTuple, string>> =
            <@
                fun (rowArray: string array) ->
                    if rowArray.Length = %%reqdArrayLenVarExpr then
                        Ok %%tupleResultVarExpr
                    else
                        Error "Incorrect array length."
            @>

        let rowArrayVar =
            match underlyingExpr with
            | Patterns.Lambda (v, _) -> v
            | _ -> failwith "Unexpected pattern."

        let rowArrayVarExpr =
            Expr.Var rowArrayVar

        let tupleElementExprs =
            List.init tupleLength (getArray rowArrayVarExpr)

        let createTupleExpr =
            Expr.NewTuple tupleElementExprs

        let rec varMapper = function
            | Patterns.Var v when v = reqdArrayLenVar ->
                Expr.Value tupleLength
            | Patterns.Var v when v = tupleResultVar ->
                createTupleExpr
            | ExprShape.ShapeLambda (var, expr) ->
                Expr.Lambda (var, varMapper expr)
            | ExprShape.ShapeCombination (o, exprs) ->
                ExprShape.RebuildShapeCombination (o, exprs |> List.map varMapper)
            | expr ->
                expr

        let instantiatorExpr =
            varMapper underlyingExpr

        LeafExpressionConverter.EvaluateQuotation instantiatorExpr
        :?> (string array -> Result<'TTuple, string>)


    [<EntryPoint>]
    let main _ =
        let instantiator =
            createInstantiator<string * string>

        printfn "%A" (instantiator [| "ABC"; "DEF"; "GHI" |])    

        0

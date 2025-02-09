
namespace AnalysisOfChangeEngine.ProviderImplementations

open FSharp.Linq.RuntimeHelpers
open FSharp.Quotations
open FSharp.Reflection
open FsToolkit.ErrorHandling



module private RowSplitter =

    // Helper function that allows us to extract a specific array element.
    // (Surprisingly, there is no Expr construct to get an array element!)
    let getArray (arr: Expr) idx =
        <@@ ((%%arr: string array))[idx] @@>

    let findHdrIdx availableHeaders hdr =
        // Determine the index of the available header which corresponds to 'hdr' above.
        let foundIdxs =
            availableHeaders
            |> Seq.indexed
            |> Seq.choose (function | idx, hdr' when hdr = hdr' -> Some idx | _ -> None)
            |> Seq.toList

        match foundIdxs with
        | [ idx ] ->
            Ok idx
        | [] ->
            Error (sprintf "Unable to locate required header '%s'." hdr)                
        | _ ->
            Error (sprintf "Multiple headers found for '%s'." hdr)


    let createCompiledSplitter<'TTuple> (sourceIdxs: int list, arrayLength)
        : string array -> Result<'TTuple, string> =
            let tupleLength =
                FSharpType.GetTupleElements(typeof<'TTuple>).Length

            if tupleLength <> sourceIdxs.Length then
                failwith "Number of tuple elements inconsistent with number of index mappings."

            let reqdArrayLenVarDef =
                Var ("reqdArrayLen", typeof<int>)

            let reqdArrayLenVar =
                Expr.Var reqdArrayLenVarDef

            let tupleResultVarDef =
                Var ("tupleResult", typeof<'TTuple>)

            let tupleResultVar =
                Expr.Var tupleResultVarDef

            let underlyingExpr: Expr<string array -> Result<'TTuple, string>> =
                <@
                    fun (rowArray: string array) ->
                        if rowArray.Length = %%reqdArrayLenVar then
                            Ok %%tupleResultVar
                        else
                            Error "Incorrect array length."
                @>

            // Extract the row array variable created within the above quotation.
            let rowArrayVar =
                match underlyingExpr with
                | Patterns.Lambda (v, _) -> v
                | _ -> failwith "Unexpected pattern."

            let rowArrayVarExpr =
                Expr.Var rowArrayVar

            // Expressions representing the value for each tuple element.
            let tupleElementExprs =
                sourceIdxs
                |> List.map (getArray rowArrayVarExpr)

            // This was previously tried using the Expr.Substitute instance method.
            // It was failing due to compilation complaining about the 'rowArray' variable
            // not being found! Very bizarre.
            // Instead, used a more 'raw' approach to accomplish this.
            let rec varMapper = function
                | Patterns.Var v when v = reqdArrayLenVarDef ->
                    Expr.Value arrayLength
                | Patterns.Var v when v = tupleResultVarDef ->
                    Expr.NewTuple tupleElementExprs
                | ExprShape.ShapeLambda (var, expr) ->
                    Expr.Lambda (var, varMapper expr)
                | ExprShape.ShapeCombination (shape, exprs) ->
                    ExprShape.RebuildShapeCombination (shape, exprs |> List.map varMapper)
                | expr ->
                    expr

            let instantiatorExpr =
                varMapper underlyingExpr

            let compiledInstantiator =
                LeafExpressionConverter.EvaluateQuotation instantiatorExpr

            downcast compiledInstantiator


// The closest we can get to a static class without using a module.
// We need a class in order to define a static member which better facilitates reflection.
// Note that class must be public in order for reflection within the type provider logic to work.
[<AbstractClass; Sealed>]
type public RowSplitter private () = 
    static member CreateSplitterBody<'TTuple> (specStr, availableHeaders) =
        result {
            let! mappings =
                ColumnMapping.parseSpecification specStr

            // These are the headers in the underlying file.
            let availableHeaders =
                availableHeaders |> Seq.toList

            let idxFinder =
                RowSplitter.findHdrIdx availableHeaders

            let columnHeadersSet =
                Set availableHeaders

            do! availableHeaders.Length
                |> Result.requireEqualTo columnHeadersSet.Count "Column headers must be distinct."

            let! sourceIdxs =
                mappings
                |> List.map _.WithinFile
                |> List.map idxFinder
                |> List.sequenceResultM

            let instantiator =
                RowSplitter.createCompiledSplitter<'TTuple> (sourceIdxs, availableHeaders.Length)

            return instantiator
        }       

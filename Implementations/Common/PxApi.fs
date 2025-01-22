
namespace AnalysisOfChangeEngine.Implementations


[<RequireQualifiedAccess>]
module PxApi =

    open System
    open System.Reflection
    open System.Text
    open FSharp.Linq.RuntimeHelpers
    open FSharp.Reflection
    open FSharp.Quotations


    [<RequireQualifiedAccess>]
    module private Exprs =
        let private constString (value: string) =
            Expr.Cast<string> (Expr.Value value)

        let ignoreExpr =
            match <@ fun (sb: StringBuilder) -> do ignore sb @> with
            | Patterns.Lambda (_, Patterns.Call (_, ignoreMI, _)) ->
                fun (expr: Expr<StringBuilder>) -> Expr.Call (ignoreMI, [expr])
            | _ -> failwith "Unexpected pattern."
                
        let private createAppenderWithTransform<'T, 'U>
            (expr: Expr<StringBuilder -> 'U -> StringBuilder>) 
            (transformer: Expr<'T> -> Expr<'U>) =
                let miAppend =
                    match expr with
                    | Patterns.Lambda (_,
                            Patterns.Lambda (_,
                                Patterns.Call (_, miAppend, _))) ->
                                    miAppend

                    | _ -> failwith "Unexpected string builder append pattern."

                fun (value: Expr<'T>) instance ->
                    Expr.Call (instance, miAppend, [ transformer value ])
                    |> Expr.Cast<StringBuilder>

        let private createAppender expr = 
            createAppenderWithTransform expr id            

        let private appendStringExpr =
            <@ fun (sb: StringBuilder) (value: string) -> sb.Append value @>

        let private appendString =
            createAppender appendStringExpr

        let createIndenter (spaces: int) =
            appendString (constString <| String.replicate spaces " ")

        let appendNewLine =
            appendString (constString Environment.NewLine)

        let private appendInt =
            createAppender <@ fun sb (value: int) -> sb.Append value @>

        let private appendReal =
            createAppender <@ fun sb (value: double) -> sb.Append value @>

        let private appendDateOnly =
            createAppenderWithTransform<DateOnly, _>
                appendStringExpr
                (fun value -> <@ (%value).ToString("O") @>)

        let private createAttributeWriter<'TValue>
            valueWriter (name: string, value: Expr) =
                seq {
                    yield appendString (constString $"<{name}>")
                    yield valueWriter value
                    yield appendString (constString $"</{name}>")
                }


        let writeStringAttribute =
            createAttributeWriter (appendString << Expr.Cast<string>)

        let writeIntAttribute =
            createAttributeWriter (appendInt << Expr.Cast<int>)

        let writeRealAttribute =
            createAttributeWriter (appendReal << Expr.Cast<double>)

        let writeDateOnlyAttribute =
            createAttributeWriter (appendDateOnly << Expr.Cast<DateOnly>)


    let createRecordSerializer<'TEntered> (spaces: int)
        : StringBuilder -> 'TEntered -> unit =
            if spaces < 0 then
                invalidArg (nameof spaces) "Cannot have negative indentation."

            let recordFields =
                FSharpType.GetRecordFields typeof<'TEntered>

            let recordVarDef =
                Var ("record", typeof<'TEntered>)

            let recordVar =
                Expr.Cast<'TEntered> (Expr.Var recordVarDef)

            let sbVarDef =
                Var ("stringBuilder", typeof<StringBuilder>)

            let sbVar =
                Expr.Cast<StringBuilder> (Expr.Var sbVarDef)

            let writerForType (pi: PropertyInfo) =
                let propertyGetter =
                    Expr.PropertyGet (recordVar, pi)

                match pi.PropertyType with
                | t when t = typeof<string> ->
                    Exprs.writeStringAttribute (pi.Name, propertyGetter)
                | t when t = typeof<int> ->
                    Exprs.writeIntAttribute (pi.Name, propertyGetter)
                | t when t = typeof<double> ->
                    Exprs.writeRealAttribute (pi.Name, propertyGetter)
                | t when t = typeof<DateOnly> ->
                    Exprs.writeDateOnlyAttribute (pi.Name, propertyGetter)
                | t ->
                    failwithf "Unsupported serialization type '%s'." t.Name

            let appendIndentation =
                Exprs.createIndenter spaces

            let combinedAppendExpr =
                recordFields
                |> Seq.collect (
                    fun pi ->
                        seq {
                            yield appendIndentation
                            yield! writerForType pi
                            yield Exprs.appendNewLine                        
                        })
                |> Seq.fold (fun combined appender -> appender combined) sbVar

            let writerForRecord =
                Expr.Lambda (sbVarDef,
                    Expr.Lambda (recordVarDef,
                        Exprs.ignoreExpr combinedAppendExpr))

            let compiledWriter =
                LeafExpressionConverter.EvaluateQuotation writerForRecord

            downcast compiledWriter
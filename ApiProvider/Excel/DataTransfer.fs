
namespace AnalysisOfChangeEngine.ApiProvider.Excel


open System
open System.Reflection
open FSharp.Linq.RuntimeHelpers
open FSharp.Quotations
open FSharp.Reflection
open Microsoft.Office.Interop

open AnalysisOfChangeEngine.Common


[<AutoOpen>]
// More of a philosophical decision... However, given these do not depend
// on the record type used to contain the various inputs, I didn't want to include
// these in the generic pseudo-static class below.
module private Common =

    let internal cellRangeVarDef =
        Var("cellRange", typeof<Excel.Range>)

    let internal cellRangeVar =
        Expr.Var cellRangeVarDef
        |> Expr.Cast<Excel.Range>

    let internal primitiveTypes = [
        typeof<int>
        typeof<float>
        typeof<float32>
        typeof<string>
        typeof<bool>
        // Interop marshalling documentation suggests that DateOnly cannot be marshalled, whereas DateTime can.
        typeof<DateTime>        
    ]

    let internal (|Primitive|_|) (t: Type) =
        primitiveTypes |> List.contains t


[<AbstractClass; Sealed>]
// TODO - Could this be internal/private? Suspect doing so might
// cause issues when compiling dynamic code.
type QuotationHelpers private () =
    // We cannot have a code quotation containing just a 'do' statement.
    // It needs to return something as done here.
    // More bizarely, if this retuns Unit, it is treated as System.Void
    // which wreaks havoc when compiling the quotation. The work-around
    // is to return something which can be ignored at the call-site.
    static member SetCellValue (cell: Excel.Range, value: obj) =
        do cell.Value null <- value
        cell

    static member ClearCellContents (cell: Excel.Range)  =
        do ignore <| cell.ClearContents ()
        cell


(*
Design Decision:
    Why use a pseudo-static class rather than a module?
    Certain methods need to be invoked via reflection. As such,
    it is easier to achieve this using vanilla static methods rather
    than let bindings within a module.

    Why the need for reflection?
    If nothing else, it allows us to enforce type-safety within a particular
    aspect of our logic. I recognise there could be a performance penalty from
    this, however, this logic is called only once up-front, so willing to accept it. 

    Also... As part of the Postgres logic, transferable types were cached
    for later re-use. Why not here? Frankly, there was a lot more complexity
    inherent in the Postgres logic. Furthermore, a given type could be used
    a significant number of times within a given product. We don't have
    that level of complexity here. As such, didn't seem worth the effort!
*)
[<AbstractClass; Sealed>]
type internal DataTransfer<'TCellInputs> private () =

    static let cellInputsVarDef =
        Var("cellInputs", typeof<'TCellInputs>)

    static let cellInputsVar =
        Expr.Var cellInputsVarDef
        |> Expr.Cast<'TCellInputs> 

    static let cellInputMembers =
        FSharpType.GetRecordFields
            (typeof<'TCellInputs>, BindingFlags.Public ||| BindingFlags.NonPublic)

    static member private CreateWriterForPrimitiveInput
        (inputPI: PropertyInfo) (cellRange: Excel.Range) (inputs: 'TCellInputs) =
            do cellRange.Value null <- inputPI.GetValue inputs

    static member private CreateWriterForNonOptionalUnionInput
        (inputPI: PropertyInfo) (cellRange: Excel.Range) (inputs: 'TCellInputs) =
            do cellRange.Value null <- sprintf "%A" (inputPI.GetValue inputs)

    static member private CreateWriterForOptionalPrimitiveInput<'TInnerValue>
        (inputPI: PropertyInfo)
        : Excel.Range -> 'TCellInputs -> unit =
            let inputGetter =
                Expr.PropertyGet (cellInputsVar, inputPI)
                |> Expr.Cast<'TInnerValue option>

            let writerExpr =
                Expr.Lambda(
                    cellRangeVarDef,
                    Expr.Lambda(
                        cellInputsVarDef,
                        <@
                        match %inputGetter with
                        | Some value ->
                            // Refer to the blurb in QuotationHelpers above as
                            // to why this is needed.
                            ignore <| QuotationHelpers.SetCellValue (%cellRangeVar, value)                            
                        | None ->
                            ignore <| QuotationHelpers.ClearCellContents (%cellRangeVar)
                        @>
                    )
                )

            downcast LeafExpressionConverter.EvaluateQuotation writerExpr

    static member val private mi_CreateWriterForOptionalPrimitiveInput =
        typeof<DataTransfer<'TCellInputs>>
            .GetMethod(
                // Fun fact... If we don't specify a property type parameter here,
                // it defaults to obj which then leads to an exception because
                // obj is NOT a record type!
                nameof(DataTransfer<'TCellInputs>.CreateWriterForOptionalPrimitiveInput),
                BindingFlags.Static ||| BindingFlags.NonPublic
            )

    static member private InvokeCreateWriterForOptionalPrimitiveInput
        // Note that the inner value type does NOT correspond to the outer
        // value type as represented by the property info argument.
        (innerValueType: Type, inputPI: PropertyInfo)
        : Excel.Range -> 'TCellInputs -> unit =
            downcast DataTransfer<'TCellInputs>
                .mi_CreateWriterForOptionalPrimitiveInput
                .MakeGenericMethod(innerValueType)
                .Invoke(null, [| inputPI |])

    // I know... Some definite duplication compared to the non-union version above.
    // However, willing to accept on aeshtetic grounds!
    static member private CreateWriterForOptionalUnionInput<'TInnerValue>
        (inputPI: PropertyInfo)
        : Excel.Range -> 'TCellInputs -> unit =
            let inputGetter =
                Expr.PropertyGet (cellInputsVar, inputPI)
                |> Expr.Cast<'TInnerValue option>

            let writerExpr =
                Expr.Lambda(
                    cellRangeVarDef,
                    Expr.Lambda(
                        cellInputsVarDef,
                        <@
                        match %inputGetter with
                        | Some value ->
                            ignore <| QuotationHelpers.SetCellValue (%cellRangeVar, sprintf "%A" value)
                        | None ->
                            ignore <| QuotationHelpers.ClearCellContents (%cellRangeVar)
                        @>
                    )
                )

            downcast LeafExpressionConverter.EvaluateQuotation writerExpr

    static member val private mi_CreateWriterForOptionalUnionInput =
        typeof<DataTransfer<'TCellInputs>>
            .GetMethod(
                nameof(DataTransfer<'TCellInputs>.CreateWriterForOptionalUnionInput),
                BindingFlags.Static ||| BindingFlags.NonPublic
            )

    static member private InvokeCreateWriterForOptionalUnionInput
        // Note that the inner value type does NOT correspond to the outer
        // value type as represented by the property info argument.
        (innerValueType: Type, inputPI: PropertyInfo)
        : Excel.Range -> 'TCellInputs -> unit =
            downcast DataTransfer<'TCellInputs>
                .mi_CreateWriterForOptionalUnionInput
                .MakeGenericMethod(innerValueType)
                .Invoke(null, [| inputPI |])

    static member private CreateWriterForInput<'TValue> (inputPI: PropertyInfo) =
        match typeof<'TValue> with
        | Primitive ->
            DataTransfer<'TCellInputs>.CreateWriterForPrimitiveInput inputPI
        | NonOptionalNonParameterizedUnion _ ->
            DataTransfer<'TCellInputs>.CreateWriterForNonOptionalUnionInput inputPI
        | Optional (Primitive as innerType) ->
            DataTransfer<'TCellInputs>.InvokeCreateWriterForOptionalPrimitiveInput (innerType, inputPI)
        | Optional (NonOptionalNonParameterizedUnion _ as innerType) ->
            DataTransfer<'TCellInputs>.InvokeCreateWriterForOptionalUnionInput (innerType, inputPI)
        | _ ->
            failwithf "Unable to create writer for type '%s'." typeof<'TValue>.FullName           

    static member val private mi_CreateWriterForInput =
        typeof<DataTransfer<'TCellInputs>>
            .GetMethod(
                nameof(DataTransfer<'TCellInputs>.CreateWriterForInput),
                BindingFlags.Static ||| BindingFlags.NonPublic
            )

    static member private InvokeCreateWriterForInput
        (inputPI: PropertyInfo)
        : Excel.Range -> 'TCellInputs -> unit =
            downcast DataTransfer<'TCellInputs>
                .mi_CreateWriterForInput
                .MakeGenericMethod(inputPI.PropertyType)
                .Invoke(null, [| inputPI |])

    static member val private deferredWriters =
        // I'm struggling to imagine a situation where this output of this
        // would never be needed. However, didn't want to assume and 
        // neglgible impact of wrapping as a lazy value.
        lazy(
            cellInputMembers
            |> Array.map (fun pi ->
                let excelRangeName =
                    getRangeNameFromPI pi

                // Create this in advance and pass-in via a closure.
                let cellWriter =
                    DataTransfer<'TCellInputs>.InvokeCreateWriterForInput pi 

                fun (workbook: Excel.Workbook) ->
                    let excelRange =
                        workbook.Names[excelRangeName].RefersToRange                       

                    cellWriter excelRange                
            )
        )

    static member MakeInputsWriter (workbook: Excel.Workbook)
        : 'TCellInputs -> unit = 
            let writers =
                DataTransfer<'TCellInputs>.deferredWriters.Value
                |> Array.map (fun dw -> dw workbook)

            fun (cellInputs: 'TCellInputs) ->
                for writer in writers do
                    do writer cellInputs            


namespace AnalysisOfChangeEngine.Implementations.Common


module PolicyRecord =

    open FSharp.Linq.RuntimeHelpers
    open FSharp.Quotations
    open FSharp.Reflection

    open AnalysisOfChangeEngine.Common


    (*let createFieldChangerWithFormatter<'TPolicyRecord, 'TField when 'TPolicyRecord :> IPolicyRecord>
        (selector: Expr<'TPolicyRecord -> 'TField>, formatter: 'TField -> string)
        : 'TField -> PolicyRecordFieldChanger<'TPolicyRecord> =
            let recordFields =
                FSharpType.GetRecordFields (typeof<'TPolicyRecord>)
                |> List.ofArray

            let currentRecordVarDef =
                Var ("policyRecord", typeof<'TPolicyRecord>)

            let currentRecordVar =
                Expr.Var currentRecordVarDef

            let propertyToReplace =
                match selector with
                | Patterns.Lambda (x,
                    Patterns.PropertyGet(Some (Patterns.Var x'), pi, [])) when x = x' -> pi
                | _ -> failwith "Unexpected pattern."

            let newValueVarDef =
                Var ("newValue", typeof<'TField>)

            let newValueVar =
                Expr.Var newValueVarDef

            let newRecordVarDef =
                Var ("newRecord", typeof<'TPolicyRecord>)

            let newRecordVar =
                Expr.Cast<'TPolicyRecord> (Expr.Var newRecordVarDef)

            let fieldExprs =
                recordFields
                |> List.map (fun pi ->
                    if pi.Name = propertyToReplace.Name then
                        newValueVar
                    else
                        Expr.PropertyGet (currentRecordVar, pi))

            let newPolicyRecordExpr =
                Expr.NewRecord (typeof<'TPolicyRecord>, fieldExprs)
                |> Expr.Cast<'TPolicyRecord>

            let beforeValueUnformattedExpr =
                Expr.PropertyGet (currentRecordVar, propertyToReplace, [])
                |> Expr.Cast<'TField>

            // Re-extract the after value as proof it has changed.
            let afterValueUnformattedExpr =
                Expr.PropertyGet (newRecordVar, propertyToReplace, [])
                |> Expr.Cast<'TField>

            let dataChangeRecordExpr: Expr<PolicyRecordDataChange> =
                <@
                    {
                        FieldChanged = propertyToReplace.Name
                        BeforeValue = formatter %beforeValueUnformattedExpr
                        AfterValue = formatter %afterValueUnformattedExpr
                    }
                @>

            let replaceExpr =
                Expr.Lambda (newValueVarDef,
                    Expr.Lambda (currentRecordVarDef,
                        Expr.Let (
                            newRecordVarDef,
                            newPolicyRecordExpr,
                            Expr.NewTuple [ newPolicyRecordExpr; dataChangeRecordExpr ])))


            let compiledReplacer =
                LeafExpressionConverter.EvaluateQuotation replaceExpr                

            downcast compiledReplacer

    let createFieldChanger<'TPolicyRecord, 'TField when 'TPolicyRecord :> IPolicyRecord>
        (selector: Expr<'TPolicyRecord -> 'TField>) =
            createFieldChangerWithFormatter<'TPolicyRecord, 'TField> (selector, string)*)
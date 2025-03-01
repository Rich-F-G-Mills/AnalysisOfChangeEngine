
namespace AnalysisOfChangeEngine.Controller


[<RequireQualifiedAccess>]
module WalkParser =

    open System.Reflection
    open FSharp.Quotations
    open AnalysisOfChangeEngine


    [<RequireQualifiedAccess>]
    type SourceElementType<'TPolicyRecord when 'TPolicyRecord :> IPolicyRecord> =
        | ApiCall of Name: string * Requestor: ApiRequest<'TPolicyRecord> * Output: PropertyInfo
        | Calculation

    [<NoEquality; NoComparison>]
    type SourceElement<'TPolicyRecord, 'TStepResults when 'TPolicyRecord :> IPolicyRecord> =
        {
            PropertyInfo    : PropertyInfo
            Type            : SourceElementType<'TPolicyRecord>
        }

    let private (|SourceLambda|_|) = function
        | Patterns.Lambda (from,
            Patterns.Lambda (prior, expr)) ->
                Some (from, prior, expr)
        | _ ->
            None

    // When constructing a record via a code quotation, if the elements are not supplied in
    // the exact same order as the original record type definition, the compiler uses (nested)
    // let bindings which are then passed into the new record expression.
    // This is (likely) just in case the member definitions have side-effects which can then
    // be executed in the same order as the member name-value pairings.
    // Given there are no side-effects to worry about here, we can collapse everything down
    // into the correctly ordered new record expression.
    let flattenSourceDefinition (source: SourceDefinition<'TPolicyRecord, 'TStepResults, 'TApiCollection>) =
        let fromVarDef, priorVarDef, sourceBody =
            match source with
            | SourceLambda (from, prior, expr) ->
                (from, prior, expr)
            | _ ->
                failwith "Unexpected source definition."

        let rec inner mappings = function
            | Patterns.Let (var, def, body) ->
                inner (mappings |> Map.add var def) body

            | Patterns.NewRecord (recType, values) ->
                let newValues =
                    values
                    |> List.map (function
                        | Patterns.Var v when mappings |> Map.containsKey v ->
                            mappings[v]
                        | expr ->
                            expr)

                Expr.NewRecord (recType, newValues)

            | _ ->
                failwith "Unexpected source definition."

        let newSourceBody =
            inner Map.empty sourceBody

        let newSource =
            Expr.Lambda (fromVarDef,
                Expr.Lambda (priorVarDef, newSourceBody))

        newSource
        |> SourceDefinition.castExpr<'TPolicyRecord, 'TStepResults, 'TApiCollection>

    let parseSource (apiCollection: 'TApiCollection) (source: SourceDefinition<_, _, 'TApiCollection>) =
        let fromVarDef, priorVarDef, sourceBody =
            match source with
            | SourceLambda (from,  prior, expr) ->
                (from, prior, expr)
            | _ ->
                failwith "Unexpected source lambda definition."

        let (|FromVarDef|_|) = function
            | v when v = fromVarDef -> Some () | _ -> None

        let (|PriorVarDef|_|) = function
            | v when v = priorVarDef -> Some () | _ -> None

        let (|FromVar|_|) = function
            | Patterns.Var FromVarDef -> Some () | _ -> None

        let (|PriorVar|_|) = function
            | Patterns.Var PriorVarDef -> Some () | _ -> None

        (*let (|ApiRequest|_|) = function
            | Patterns.Call (Some FromVar, apiCallMI, [
                Patterns.PropertyGet (Some ApiVar, wrappedRequestorPI, [])
                Patterns.Lambda (_, Patterns.PropertyGet (_, apiOutputPI, []))]) ->
                    let (unwrapped: IUnwrappableApiRequest<'TPolicyRecord>) =
                        downcast wrappedRequestorPI.GetValue apiCollection

                    Some (SourceElementType.ApiCall (unwrapped.Name, unwrapped.Requestor, apiOutputPI))
         *)          

        0
            
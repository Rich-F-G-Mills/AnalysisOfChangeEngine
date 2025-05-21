
namespace AnalysisOfChangeEngine.DataStore.Postgres.DataTransferObjects

open System
open System.Reflection
open FSharp.Linq.RuntimeHelpers
open FSharp.Quotations
open FSharp.Reflection
open FsToolkit.ErrorHandling

open AnalysisOfChangeEngine.DataStore.Postgres


type internal ITransferableType =
    interface
        abstract member UnderlyingType      : Type with get
        abstract member ToSqlParamValueExpr : Expr with get
        abstract member ToSqlParamSite      : paramName: string -> string
        abstract member ToSqlParamSiteExpr  : Expr<string -> string>
        abstract member ToSqlLiteralExpr    : Expr with get
        abstract member ToSqlSelector       : string -> string
        abstract member ReadSqlColumnExpr   : Expr<RowReader> * string -> Expr
    end

// The generic version is more usueful when it comes to the type-safety it provides.
type internal ITransferableType<'TValue> =
    interface
        inherit ITransferableType

        abstract member ToSqlParamValueExpr : Expr<'TValue -> SqlValue> with get
        abstract member ToSqlParamValue     : 'TValue -> SqlValue
        abstract member ToSqlLiteralExpr    : Expr<'TValue -> string> with get
        abstract member ReadSqlColumnExpr   : Expr<RowReader> * string -> Expr<'TValue>
    end


(*
Design Decision:
    In the case of enumerations, we will need to be able to invoke the corresponding
    transferable type generator via reflection. Easier to locate and invoke
    the required function if we use a pseudo-static class rather than an F# module.
*)
[<AbstractClass; Sealed>]
type private TransferableTypeFactory private () =
    
    static member internal createFor<'TValue>
        colSelector colReaderExpr toSqlParamValueExpr toSqlParamSiteExpr toSqlLiteralExpr
        : ITransferableType =
            // Prevents the user from having to supply both the Expr and non-Expr versions/
            // TODO - Could this be done using the ReflectedDefinition attribute?
            let toSqlParamValue: 'TValue -> SqlValue =
                downcast LeafExpressionConverter.EvaluateQuotation toSqlParamValueExpr  
                
            let toSqlParamSite: string -> string =
                downcast LeafExpressionConverter.EvaluateQuotation toSqlParamSiteExpr

            let transferableType =
                {
                    new ITransferableType<'TValue> with
                        member _.ToSqlParamValueExpr with get()         = toSqlParamValueExpr
                        member _.ToSqlParamValue value                  = toSqlParamValue value
                        member _.ToSqlLiteralExpr with get()            = toSqlLiteralExpr                  
                        member _.ReadSqlColumnExpr (rrExpr, colName)    = colReaderExpr rrExpr colName                    

                    interface ITransferableType with                                            
                        member _.UnderlyingType                         = typeof<'TValue>
                        member _.ToSqlParamSiteExpr with get()          = toSqlParamSiteExpr
                        member _.ToSqlParamSite paramName               = toSqlParamSite paramName
                        member _.ToSqlSelector colName                  = colSelector colName
                        member _.ToSqlParamValueExpr with get()         = toSqlParamValueExpr
                        member _.ToSqlLiteralExpr   with get()          = toSqlLiteralExpr                    
                        member _.ReadSqlColumnExpr (rrExpr, colName)    = colReaderExpr rrExpr colName                                  
                }

            upcast transferableType

    // Here we assume that the supplied type parameter is for the discriminated
    // union we're looking to serialise to the database.
    // Note that this returns a transferable type for the DU and its optional alter-ego.
    static member internal createForEnum<'TEnum> (schema, pgEnumName) =
        let enumType =
            typeof<'TEnum>

        let caseMapping =
            FSharpType.GetUnionCases (enumType, BindingFlags.NonPublic)
            |> Seq.map (fun uc -> uc.Name, uc)
            |> Map.ofSeq

        // We need to ensure that our enumeration cases are non-parameterised.
        assert
            caseMapping
            |> Map.forall (fun _ uc -> uc.GetFields().Length = 0)

        let enumStrValueVarDef =
            Var ("enumStrValue", typeof<string>)

        let enumStrValueVar =
            Expr.Var enumStrValueVarDef
            |> Expr.Cast<string>

        let caseExprs =
            caseMapping
            |> Map.toSeq
            |> Seq.map (fun (enumStrValue, enumCaseInfo) ->
                {|
                    StrVsStr =
                        <@ %enumStrValueVar = enumStrValue @>
                    CaseCreation =
                        Expr.NewUnionCase(enumCaseInfo, [])
                        |> Expr.Cast<'TEnum>
                |})
            |> Seq.toList      
                    
        let colSelector =
            sprintf "%s::text"

        // If we get supplied text that doesn't correspond to one of our DU levels...
        // This will intentionally fail as that suggests a discrepency between our
        // view of the enumeration here and the underlying database type.
        let stringToEnumMappingLambdaExpr =
            Expr.Lambda(
                enumStrValueVarDef,
                caseExprs
                |> Seq.fold
                    (fun elseExpr caseExpr ->
                        Expr.IfThenElse(caseExpr.StrVsStr, caseExpr.CaseCreation, elseExpr)
                        |> Expr.Cast<'TEnum>)
                    <@ failwith "Unable to transfer enumeration case." @>
            )
            |> Expr.Cast<string -> 'TEnum>            

        let colReaderExpr (r: Expr<RowReader>) colName =
            <@ (%stringToEnumMappingLambdaExpr) ((%r).string colName) @>

        let optionalColReaderExpr (r: Expr<RowReader>) colName =
            <@ Option.map %stringToEnumMappingLambdaExpr ((%r).stringOrNone colName) @>

        let toSqlParamValueExpr =
            // Given we're not checking the available enumeration values from the Postgres side,
            // no reason not to just use the %A formatter below.
            // TODO - Could we query the underlying database for the available values?
            <@ fun (enumValue: 'TEnum) -> SqlValue.String (sprintf "%A" enumValue) @>

        let optionalToSqlParamValueExpr =
            // Given we're not checking the available enumeration values from the Postgres side,
            // no reason not to just use the %A formatter below.
            <@ Option.either (SqlValue.String << sprintf "%A") (fun _ -> SqlValue.Null) @>
                
        let toSqlParamSite =
            <@ fun paramName -> sprintf "%s::\"%s\".\"%s\"" paramName schema pgEnumName @>

        let toSqlLiteralExpr =
            // Same situation as for the parameter value logic above.
            <@ fun enumValue -> sprintf "'%A'::\"%s\".\"%s\"" enumValue schema pgEnumName @>

        let optionalToSqlLiteralExpr =
            <@ Option.either
                (fun enumValue -> sprintf "'%A'::\"%s\".\"%s\"" enumValue schema pgEnumName)
                (fun _ -> sprintf "NULL::\"%s\".\"%s\"" schema pgEnumName) @>

        let transferableType =
            TransferableTypeFactory.createFor<'TEnum>
                colSelector colReaderExpr toSqlParamValueExpr toSqlParamSite toSqlLiteralExpr

        let optionalTransferableType =
            TransferableTypeFactory.createFor<'TEnum option>
                colSelector optionalColReaderExpr optionalToSqlParamValueExpr toSqlParamSite optionalToSqlLiteralExpr

        transferableType, optionalTransferableType


[<RequireQualifiedAccess>]
module internal TransferableType =
    
    open System.Collections.Generic


    let private makePrimitiveTransferType<'TValue> colReaderExpr toSqlParamValueExpr toSqlLiteralExpr =
        TransferableTypeFactory.createFor<'TValue> id colReaderExpr toSqlParamValueExpr <@ id @> toSqlLiteralExpr

    let private primitiveTransferTypes =
        [
            makePrimitiveTransferType<bool>
                (fun r c -> <@ (%r).bool c @>)
                <@ SqlValue.Bool @>
                <@ sprintf "%b" @>

            makePrimitiveTransferType<Int16>
                (fun r c -> <@ (%r).int16 c @>)
                <@ int >> SqlValue.Int @>
                <@ sprintf "%i" @>

            makePrimitiveTransferType<Int16 option>
                (fun r c -> <@ (%r).int16OrNone c @>)
                <@ Option.either (SqlValue.Int << int) (fun _ -> SqlValue.Null) @>
                <@ Option.either (sprintf "%i") (fun _ -> "NULL") @>

            makePrimitiveTransferType<int>
                (fun r c -> <@ (%r).int c @>)
                <@ SqlValue.Int @>
                <@ sprintf "%i" @>

            makePrimitiveTransferType<Guid>
                (fun r c -> <@ (%r).uuid c @>)
                <@ SqlValue.Uuid @>
                <@ sprintf "'%O'" @>

            makePrimitiveTransferType<Guid option>
                (fun r c -> <@ (%r).uuidOrNone c @>)
                <@ SqlValue.UuidOrNull @>
                <@ Option.either (sprintf "'%O'") (fun _ -> "NULL") @>

            makePrimitiveTransferType<string>
                (fun r c -> <@ (%r).string c @>)
                <@ SqlValue.String @>
                <@ sprintf "'%s'" @>

            makePrimitiveTransferType<string option>
                (fun r c -> <@ (%r).stringOrNone c @>)
                <@ SqlValue.StringOrNull @>
                <@ Option.either (sprintf "'%s'") (fun _ -> "NULL") @> 

            makePrimitiveTransferType<DateOnly>
                (fun r c -> <@ (%r).dateOnly c @>)
                <@ Choice2Of2 >> SqlValue.Date @>
                <@ _.ToString("O") >> sprintf "'%s'::date" @>

            makePrimitiveTransferType<DateTime>
                (fun r c -> <@ (%r).dateTime c @>)
                <@ SqlValue.Timestamp @>
                <@ _.ToString("O") >> sprintf "'%s'::timestamp" @>

            makePrimitiveTransferType<float32>
                (fun r c -> <@ (%r).float c @>)
                <@ SqlValue.Real @>
                <@ sprintf "%f" @>
        ]

    let private enumTransferTypeMI =
        typeof<TransferableTypeFactory>
        |> _.GetMethod(
                nameof(TransferableTypeFactory.createForEnum),
                BindingFlags.Static ||| BindingFlags.NonPublic)

    let private invokeEnumTransferType (t: Type, schema: string, pgEnumName: string)
        : ITransferableType * ITransferableType =
            // You'd think we'd need to supply parameters as a tuple... But no!
            downcast enumTransferTypeMI.MakeGenericMethod(t).Invoke(null, [| schema; pgEnumName |])

    let private (|KnownPrimitiveType|_|) t =
        primitiveTransferTypes
        |> List.tryPick (function | tt when t = tt.UnderlyingType -> Some tt | _ -> None)

    let private (|EnumType|_|) schema t =
        option {
            do! Option.requireTrue (FSharpType.IsUnion (t, BindingFlags.NonPublic))

            let pgCommonEnumAttrib =
                t.GetCustomAttribute<PostgresCommonEnumerationAttribute> (true)
                |> Option.ofNull

            let pgProductSpecificEnumAttrib =
                t.GetCustomAttribute<PostgresProductSpecificEnumerationAttribute> (true)
                |> Option.ofNull

            // We must have one or the other of these attributes.
            match pgCommonEnumAttrib, pgProductSpecificEnumAttrib with
            | Some pgCommonEnumAttrib', None ->
                return "common", pgCommonEnumAttrib'.TypeName
            | None, Some pgProductSpecificEnumAttrib' ->
                return schema, pgProductSpecificEnumAttrib'.TypeName
            | _ ->
                return! None
        }

    let private (|Optional|_|) (t: Type) =
        option {
            do! Option.requireTrue t.IsGenericType

            let genericTypeDef =
                t.GetGenericTypeDefinition()

            do! Option.requireTrue (genericTypeDef = typedefof<_ option>)

            let innerType =
                t.GenericTypeArguments[0]

            return innerType
        }

    let private (|MaybeOptionalEnumType|_|) schema : Type -> _ = function
        // Check if we're optional first.
        | Optional (EnumType schema (schemaToUse, pgEnumName) as enumType) ->
            Some (enumType, schemaToUse, pgEnumName)
        | EnumType schema (schemaToUse, pgEnumName) as enumType ->
            Some (enumType, schemaToUse, pgEnumName)
        | _ ->
            None

    // Given the transferable type for an enumeration also depends on the schema,
    // we need to include that in the key as well.
    let private cachedEnumTypes =
        new Dictionary<Type * string, ITransferableType>()

    let internal getForType (schema: string) = function
        | KnownPrimitiveType tt ->
            tt

        | MaybeOptionalEnumType schema (enumType, schemaToUse, pgEnumName) as t ->
            match cachedEnumTypes.TryGetValue((t, schemaToUse)) with
            | true, tt ->
                tt

            | false, _ ->
                // If we're not already cached... We need to go through the motions...
                let tt, optionalTt =
                    invokeEnumTransferType (enumType, schemaToUse, pgEnumName)

                let asOptionalType =
                    typedefof<_ option>.MakeGenericType(enumType)

                // Given we have the transferable type for the DU and its optional alter-ego, we'll
                // add them both while we have them.
                do cachedEnumTypes.Add ((enumType, schemaToUse), tt)
                do cachedEnumTypes.Add ((asOptionalType, schemaToUse), optionalTt)

                // Now we need to figure out which one we need to return...
                if t = enumType then
                    tt
                elif t = asOptionalType then
                    optionalTt
                else
                    failwith "Unexpected error."
 
        | t ->
            failwithf "Unable to create transferable type for '%s'." t.Name


namespace AnalysisOfChangeEngine.DataStore.Postgres.DataTransferObjects

open System
open System.Collections.Concurrent
open System.Data.Common
open System.Reflection
open FSharp.Linq.RuntimeHelpers
open FSharp.Quotations
open FSharp.Reflection
open FsToolkit.ErrorHandling
open Npgsql

open AnalysisOfChangeEngine.DataStore.Postgres


type internal ITransferableType =
    interface
        abstract member UnderlyingType              : Type with get
        abstract member ToSqlParamValueExpr         : ProductSchemaName -> Expr
        abstract member ToSqlParamValueArrayExpr    : ProductSchemaName -> Expr
        abstract member SqlColMapper                : string -> string
        abstract member ReadSqlColumnExpr           : Expr<DbDataReader> * int -> Expr
    end

// The generic version is more usueful when it comes to the type-safety it provides.
type internal ITransferableType<'TValue> =
    interface
        inherit ITransferableType

        // We use the non-generic version of the Sql parameter as it won't always have the
        // same generic type as that used in the underlying DTO member type.
        // eg... Such as for enums where the underlying type is a string.
        abstract member ToSqlParamValueExpr         : ProductSchemaName -> Expr<'TValue -> NpgsqlParameter>
        abstract member ToSqlParamValueArrayExpr    : ProductSchemaName -> Expr<'TValue array -> NpgsqlParameter>
        abstract member ToSqlParamValue             : ProductSchemaName -> ('TValue -> NpgsqlParameter)
        abstract member ToSqlParamValueArray        : ProductSchemaName -> ('TValue array -> NpgsqlParameter)
        abstract member ReadSqlColumnExpr           : Expr<DbDataReader> * int -> Expr<'TValue>
    end


[<RequireQualifiedAccess>]
module private CachedByProductSchema =

    let make (fn: ProductSchemaName -> 'T) =
        // In theory, we could make this non-concurrent as this code is most likely to
        // be called via assembly start-up logic, which is single-threaded.
        // However, with this minor change, can make it easily support parallel execution.
        let cachedValues =
            new ConcurrentDictionary<string, 'T> ()

        fun (ProductSchemaName productSchema' as productSchema) ->
            // In theory, this will lead to allocations as the getter factory is using a closure.
            // However, for the sake of aesthetic beauty, we'll live with it.
            cachedValues.GetOrAdd (productSchema', fun _ -> fn productSchema)


[<AbstractClass; Sealed>]
type internal TransferableType private () =

    static let cachedTransferableTypes =
        new ConcurrentDictionary<Type, ITransferableType>()

    static let (|NonOptionalNonParameterizedUnion|_|) (unionType: Type) =
        option {
            do! Option.requireTrue (FSharpType.IsUnion (unionType, BindingFlags.NonPublic))

            let recordColumns =
                FSharpType.GetUnionCases (unionType, BindingFlags.NonPublic)

            let allNonParameterized =
                recordColumns
                |> Array.forall (fun uc -> uc.GetFields().Length = 0)

            do! Option.requireTrue allNonParameterized

            let! pgEnumAttrib =
                unionType.GetCustomAttribute<PostgresEnumerationAttribute> (true)
                |> Option.ofNull

            let mapper =
                match pgEnumAttrib.Location with
                | PostgresEnumerationSchema.Common ->
                    fun _ -> EnumSchemaName "common"
                | PostgresEnumerationSchema.ProductSpecific ->
                    fun (ProductSchemaName name) -> EnumSchemaName name

            return PgTypeName pgEnumAttrib.PgTypeName, mapper
        }

    static let (|NonOptionalNonUnion|_|) (nonOptionalType: Type) =
        Option.requireFalse (FSharpType.IsUnion (nonOptionalType, BindingFlags.NonPublic))

    static let (|Optional|_|) (maybeOptionalType: Type) =
        option {
            do! Option.requireTrue maybeOptionalType.IsGenericType

            let genericTypeDef =
                maybeOptionalType.GetGenericTypeDefinition()

            do! Option.requireTrue (genericTypeDef = typedefof<_ option>)

            let innerType =
                maybeOptionalType.GenericTypeArguments[0]

            return innerType
        }

    // We cannot re-use this... If a query results in multiple null parameters,
    // NPGSQL complains if we use the same object instance more than once.
    static member makeNullParameter () =
        new NpgsqlParameter (Value = DBNull.Value)

    static member makeTypedParameter<'T> (value: 'T) : NpgsqlParameter =
        // Previously, the code below was being used directly within a code quotation.
        // However, the runtime expression compiler could not cope with the resulting logic.
        // A simple solution was to wrap the logic in a static member that CAN be called.
        upcast new NpgsqlParameter<'T> (TypedValue = value)

    static member makeTypedParameter<'T> (value: 'T, dataTypeName: string) : NpgsqlParameter =
        // As above. Using multiple dispatch for those instances where a data type name is needed.
        upcast new NpgsqlParameter<'T> (TypedValue = value, DataTypeName = dataTypeName)

    // Cannot use let binding as generic. Same applies to the following below.
    static member private CreateFor<'TValue> colMapper colReaderExpr toSqlParamValueExpr toSqlParamValueArrayExpr =
        // Here we cache various expressions by product schema. In absence of any
        // dependence on the prevailing product schema, we _might_ have just used lazy
        // values instead.
        let toSqlParamValueExpr' =
            CachedByProductSchema.make toSqlParamValueExpr

        let toSqlParamValueArrayExpr' =
            CachedByProductSchema.make toSqlParamValueArrayExpr

        let toSqlParamValue' =
            CachedByProductSchema.make (fun productSchema ->
                // Might as well re-use the expression we created above!
                downcast LeafExpressionConverter.EvaluateQuotation (toSqlParamValueExpr' productSchema))

        let toSqlParamValueArray' =
            CachedByProductSchema.make (fun productSchema ->
                downcast LeafExpressionConverter.EvaluateQuotation (toSqlParamValueArrayExpr' productSchema))

        {
            new ITransferableType<'TValue> with
                member _.ToSqlParamValueExpr productSchema      = toSqlParamValueExpr' productSchema
                member _.ToSqlParamValueArrayExpr productSchema = toSqlParamValueArrayExpr' productSchema
                member _.ToSqlParamValue productSchema          = toSqlParamValue' productSchema
                member _.ToSqlParamValueArray productSchema     = toSqlParamValueArray' productSchema
                member _.ReadSqlColumnExpr (rrExpr, colIdx)     = colReaderExpr rrExpr colIdx                    

            interface ITransferableType with                                            
                member _.UnderlyingType                         = typeof<'TValue>
                member _.SqlColMapper colSpec                   = colMapper colSpec
                member _.ToSqlParamValueExpr productSchema      = toSqlParamValueExpr' productSchema
                member _.ToSqlParamValueArrayExpr productSchema = toSqlParamValueArrayExpr' productSchema
                member _.ReadSqlColumnExpr (rrExpr, colIdx)     = colReaderExpr rrExpr colIdx                                  
        }

    static member private CreateForNonOptionalNonUnion<'TNonOptionalValue> () =
        let colReaderExpr (r: Expr<DbDataReader>) colIdx =
            <@ (%r).GetFieldValue<'TNonOptionalValue> colIdx @>

        // This is independent of the product schema, so we can re-use this as needed.
        let toSqlParamValueExpr': Expr<_ -> NpgsqlParameter> =
            <@
            fun value ->
                TransferableType.makeTypedParameter<_> value
            @>

        let toSqlParamValueArrayExpr': Expr<_ -> NpgsqlParameter> =
            <@
            fun value ->
                TransferableType.makeTypedParameter<_ array> value
            @>

        TransferableType.CreateFor<'TNonOptionalValue>
            id colReaderExpr (fun _ -> toSqlParamValueExpr') (fun _ -> toSqlParamValueArrayExpr')

    static member private CreateForNonOptionalUnion<'TNonOptionalUnion> (PgTypeName pgTypeName', mapper) =
        let nonOptionalUnionType =
            typeof<'TNonOptionalUnion>

        let recordColumns =
            FSharpType.GetUnionCases (nonOptionalUnionType, BindingFlags.NonPublic)

        let columnProps =
            recordColumns
            |> Array.map (fun uc ->
                let unionName =
                    Expr.Cast<string> <| Expr.Value uc.Name            

                {|
                    StringComparer =
                        // Although we could capture the variable expression via a closure,
                        // this makes it explicit what variable is being used.
                        fun strRepVar ->
                            <@ (%strRepVar) = %unionName @>
                    ValueComparer =
                        // As above.
                        fun (unionValVar: Expr<'TNonOptionalUnion>) ->
                            Expr.Cast<bool> <| Expr.UnionCaseTest (unionValVar, uc)
                    CaseFactory =
                        Expr.Cast<'TNonOptionalUnion> <| Expr.NewUnionCase (uc, [])
                |})

        let strRepVarDef =
            Var ("strRep", typeof<string>)            

        let strRepVar =
            Expr.Cast<string> <| Expr.Var strRepVarDef

        let colReaderExpr (r: Expr<DbDataReader>) colIdx =
            Expr.Let(
                strRepVarDef,
                <@ (%r).GetString colIdx @>,
                columnProps
                |> Seq.fold
                    (fun onElse p ->
                        Expr.IfThenElse (p.StringComparer strRepVar, p.CaseFactory, onElse)
                        |> Expr.Cast<'TNonOptionalUnion>)
                    <@ failwith "Unable to convert union case." @>
            )
            |> Expr.Cast<'TNonOptionalUnion>

        let toSqlParameterValueExpr (productSchema: ProductSchemaName): Expr<_ -> NpgsqlParameter> =
            let (EnumSchemaName enumSchema') =
                mapper productSchema

            let dataTypeName =
                sprintf "%s.%s" enumSchema' pgTypeName'

            <@
            fun (unionVal: 'TNonOptionalUnion) ->
                let strRep =
                    // Given we're not actively checking against the available enumeration cases,
                    // we might as well just use our own string representation.
                    sprintf "%A" unionVal

                TransferableType.makeTypedParameter<string>
                    (strRep, dataTypeName)
            @>

        let toSqlParameterValueArrayExpr (productSchema: ProductSchemaName): Expr<_ -> NpgsqlParameter> =
            let (EnumSchemaName enumSchema') =
                mapper productSchema

            let dataTypeName =
                sprintf "%s.%s[]" enumSchema' pgTypeName'

            <@
            fun (unionVals: 'TNonOptionalUnion array) ->
                let strReps =
                    unionVals
                    |> Array.map (sprintf "%A")

                TransferableType.makeTypedParameter<string array>
                    (strReps, dataTypeName)
            @>

        TransferableType.CreateFor<'TNonOptionalUnion>
            (sprintf "%s::text") colReaderExpr toSqlParameterValueExpr toSqlParameterValueArrayExpr

    static member private CreateForOptional<'TInnerType> () =
        let transferableType =
            TransferableType.GetFor<'TInnerType> ()

        let colReaderExpr r colIdx =
            let innerColReaderExpr =
                transferableType.ReadSqlColumnExpr (r, colIdx)

            <@
            if (%r).IsDBNull colIdx then
                None
            else
                Some (%innerColReaderExpr)
            @>

        let toSqlParamValueExpr productSchema: Expr<_ -> NpgsqlParameter> =
            let innerToSqlParamValueExpr =
                transferableType.ToSqlParamValueExpr productSchema

            <@
            // Here we're injecting a lambda expression directly into our optional map.
            Option.map %innerToSqlParamValueExpr
            // There isn't any issue capturing a static member for use in this way.
            >> Option.defaultWith TransferableType.makeNullParameter
            @>

        let toSqlParamValueArrayExpr _: Expr<_ -> NpgsqlParameter> =
            let typeName =
                typeof<'TInnerType>.FullName

            <@
            failwithf "Cannot convert optional type '%s' into an parameter array." typeName
            @>

        TransferableType.CreateFor<'TInnerType option>
            // Just because it's NULL-able, we can still use the inner column's mapper.
            transferableType.SqlColMapper colReaderExpr toSqlParamValueExpr toSqlParamValueArrayExpr

    static member val private mi_CreateForOptional =
        typeof<TransferableType>
            .GetMethod(
                nameof(TransferableType.CreateForOptional),
                // Will not find method without these binding flags.
                BindingFlags.Static ||| BindingFlags.NonPublic
            )

    static member private InvokeCreateForOptional (innerType: Type) : ITransferableType =
        downcast TransferableType.mi_CreateForOptional
            .MakeGenericMethod(innerType)
            .Invoke(null, Array.empty)

    static member GetFor<'TValue> ()
        : ITransferableType<'TValue> =
            let valueType =
                typeof<'TValue>

            downcast cachedTransferableTypes.GetOrAdd (valueType, fun _ ->
                let transferableType =
                    match valueType with
                    | NonOptionalNonUnion ->
                        TransferableType.CreateForNonOptionalNonUnion<'TValue> ()

                    | NonOptionalNonParameterizedUnion (pgTypeName, schemaMapper) ->
                        TransferableType.CreateForNonOptionalUnion<'TValue>
                            (pgTypeName, schemaMapper)

                    | Optional innerType ->
                        // No way around this. We don't have a way to unwrap the outer layer
                        // of the optional type represented by the generic.
                        downcast TransferableType.InvokeCreateForOptional innerType

                    | _ ->
                        failwithf "Unable to create transferable type for '%s'." valueType.FullName

                transferableType :> ITransferableType)
                
    static member val private mi_GetFor =
        typeof<TransferableType>
            .GetMethod(
                nameof(TransferableType.GetFor),
                BindingFlags.Static ||| BindingFlags.NonPublic
            )

    static member InvokeGetFor (valueType: Type) : ITransferableType =
        downcast TransferableType.mi_GetFor
            .MakeGenericMethod(valueType)
            .Invoke(null, Array.empty)            

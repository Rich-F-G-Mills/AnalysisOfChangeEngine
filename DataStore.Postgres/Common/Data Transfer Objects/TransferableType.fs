
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

open AnalysisOfChangeEngine.Common
open AnalysisOfChangeEngine.DataStore.Postgres


type internal ITransferableType =
    interface
        abstract member UnderlyingType              : Type with get
        abstract member ToSqlParamValueExpr         : Expr with get
        abstract member ToSqlParamValueArrayExpr    : Expr with get
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
        abstract member ToSqlParamValueExpr         : Expr<'TValue -> NpgsqlParameter> with get
        abstract member ToSqlParamValueArrayExpr    : Expr<'TValue array -> NpgsqlParameter> with get
        abstract member ToSqlParamValue             : 'TValue -> NpgsqlParameter
        abstract member ToSqlParamValueArray        : 'TValue array -> NpgsqlParameter
        abstract member ReadSqlColumnExpr           : Expr<DbDataReader> * int -> Expr<'TValue>
    end


(*
Design Decision:
    Why not use a module instead of a pseudo-static class?
    A subset of the methods need to be discoverable via reflection,
    which is easier to wrangle with vanilla (static) members.
*)
[<AbstractClass; Sealed>]
type internal TransferableType private () =

    static let cachedTransferableTypes =
        // Making this concurrent is probably overkill. However, useful to have just in case!
        new ConcurrentDictionary<Type, ITransferableType>()

    static let (|NonOptionalNonParameterizedPostgresUnion|_|) (t: Type) =
        match t with
        | NonOptionalNonParameterizedUnion _ ->
            option {
                let! pgEnumAttrib =
                    t.GetCustomAttribute<PostgresEnumerationAttribute> (true)
                    |> Option.ofNull

                return PgTypeName pgEnumAttrib.PgTypeName, pgEnumAttrib.Schema
            }

        | _ ->
            None

    // We cannot re-use this... If a query results in multiple null parameters,
    // NPGSQL complains if we use the same object instance more than once.
    static member makeNullParameter () =
        new NpgsqlParameter (Value = DBNull.Value)

    static member makeTypedParameter<'T> (value) : NpgsqlParameter =
        // Previously, the code below was being used directly within a code quotation.
        // However, the runtime expression compiler could not cope with the resulting logic.
        // A simple solution was to wrap the logic in a static member that CAN be
        // included and compiled within a quotation.
        upcast new NpgsqlParameter<'T> (TypedValue = value)

    static member makeTypedParameter<'T> (value, dataTypeName) : NpgsqlParameter =
        // As above. Using function overloads for those instances where a data type name is needed.
        upcast new NpgsqlParameter<'T> (TypedValue = value, DataTypeName = dataTypeName)

    // Cannot use let binding as generic. Same applies to the following below.
    static member private CreateFor<'TValue> colMapper colReaderExpr toSqlParamValueExpr toSqlParamValueArrayExpr =
        let toSqlParamValue' =
            downcast LeafExpressionConverter.EvaluateQuotation toSqlParamValueExpr

        let toSqlParamValueArray' =
            downcast LeafExpressionConverter.EvaluateQuotation toSqlParamValueArrayExpr

        {
            new ITransferableType<'TValue> with
                member _.ToSqlParamValueExpr                    = toSqlParamValueExpr
                member _.ToSqlParamValueArrayExpr               = toSqlParamValueArrayExpr
                member _.ToSqlParamValue value                  = toSqlParamValue' value
                member _.ToSqlParamValueArray values            = toSqlParamValueArray' values
                member _.ReadSqlColumnExpr (rrExpr, colIdx)     = colReaderExpr rrExpr colIdx                    

            interface ITransferableType with                                            
                member _.UnderlyingType                         = typeof<'TValue>
                member _.SqlColMapper colSpec                   = colMapper colSpec
                member _.ToSqlParamValueExpr                    = toSqlParamValueExpr
                member _.ToSqlParamValueArrayExpr               = toSqlParamValueArrayExpr
                member _.ReadSqlColumnExpr (rrExpr, colIdx)     = colReaderExpr rrExpr colIdx                                  
        }

    static member private CreateForNonOptionalNonUnion<'TNonOptionalValue> () =
        let colReaderExpr (r: Expr<DbDataReader>) colIdx =
            <@ (%r).GetFieldValue<'TNonOptionalValue> colIdx @>

        // This is independent of the product schema, so we can re-use this as needed.
        // (I know... Could have used a point-free style here)
        let toSqlParamValueExpr': Expr<_ -> NpgsqlParameter> =
            <@
            fun value ->
                TransferableType.makeTypedParameter<_> value
            @>

        let toSqlParamValueArrayExpr': Expr<_ -> NpgsqlParameter> =
            <@
            fun values ->
                TransferableType.makeTypedParameter<_ array> values
            @>

        TransferableType.CreateFor<'TNonOptionalValue>
            // Note that there is no dependency on the prevailing product schema.
            id colReaderExpr toSqlParamValueExpr' toSqlParamValueArrayExpr'

    static member private CreateForNonOptionalUnion<'TNonOptionalUnion> (PgTypeName pgTypeName', enumSchema) =
        let nonOptionalUnionType =
            typeof<'TNonOptionalUnion>

        let recordColumns =
            FSharpType.GetUnionCases
                (nonOptionalUnionType, BindingFlags.Public ||| BindingFlags.NonPublic)

        let columnProps =
            recordColumns
            |> Array.map (fun uc ->
                let unionName =
                    Expr.Value uc.Name
                    |> Expr.Cast<string>

                {|
                    StringComparer =
                        // Although we could capture the variable expression via a closure,
                        // this makes it explicit what variable is being used.                       
                        fun strRepVar ->
                            <@ (%strRepVar) = %unionName @>
                    ValueComparer =
                        // As above.
                        fun (unionValVar: Expr<'TNonOptionalUnion>) ->
                            Expr.UnionCaseTest (unionValVar, uc)
                            |> Expr.Cast<bool>
                    CaseFactory =
                        Expr.NewUnionCase (uc, [])
                        |> Expr.Cast<'TNonOptionalUnion>
                |})

        let strRepVarDef =
            Var ("strRep", typeof<string>)            

        let strRepVar =
            Expr.Var strRepVarDef
            |> Expr.Cast<string>

        let colReaderExpr (r: Expr<DbDataReader>) colIdx =
            Expr.Let(
                strRepVarDef,
                <@ (%r).GetString colIdx @>,
                columnProps
                |> Seq.fold
                    (fun onElse p ->
                        Expr.IfThenElse (p.StringComparer strRepVar, p.CaseFactory, onElse)
                        |> Expr.Cast<'TNonOptionalUnion>)
                    <@ failwithf "Unable to convert union case '%s'." (%strRepVar) @>
            )
            |> Expr.Cast<'TNonOptionalUnion>

        let toSqlParameterValueExpr: Expr<_ -> NpgsqlParameter> =
            let dataTypeName =
                sprintf "%s.%s" enumSchema pgTypeName'

            <@
            fun (unionVal: 'TNonOptionalUnion) ->
                let strRep =
                    // Given we're not actively checking against the available enumeration cases,
                    // we might as well just use our own string representation.
                    sprintf "%A" unionVal

                TransferableType.makeTypedParameter<string>
                    (strRep, dataTypeName)
            @>

        let toSqlParameterValueArrayExpr: Expr<_ -> NpgsqlParameter> =
            let dataTypeName =
                sprintf "%s.%s[]" enumSchema pgTypeName'

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

        let toSqlParamValueExpr =              
            <@
            // Here we're injecting a lambda expression directly into our optional map.
            Option.map %(transferableType.ToSqlParamValueExpr)
            // There isn't any issue capturing a static member for use in this way.
            >> Option.defaultWith TransferableType.makeNullParameter
            @>

        let toSqlParamValueArrayExpr =
            let typeName =
                typeof<'TInnerType>.FullName

            <@
            // Exception must be deferred or run-time compilation will fail.
            fun _ ->
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
            ) with get

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
                        // There is no guarantee this will work. Ultimately depends
                        // on what types Npgsql can natively serialize.
                        TransferableType.CreateForNonOptionalNonUnion<'TValue> ()

                    | NonOptionalNonParameterizedPostgresUnion (pgTypeName, enumSchema) ->
                        TransferableType.CreateForNonOptionalUnion<'TValue>
                            (pgTypeName, enumSchema)

                    | Optional innerType ->
                        // No obvious way around this. We don't have a(n obvious) way to peel
                        // away the outer layer of the optional type represented by the generic.
                        downcast TransferableType.InvokeCreateForOptional innerType

                    | _ ->
                        failwithf "Unable to create transferable type for '%s'." valueType.FullName

                transferableType :> ITransferableType)
                
    static member val private mi_GetFor =
        typeof<TransferableType>
            .GetMethod(
                nameof(TransferableType.GetFor),
                BindingFlags.Static ||| BindingFlags.NonPublic
            ) with get

    static member InvokeGetFor (valueType: Type) : ITransferableType =
        downcast TransferableType.mi_GetFor
            .MakeGenericMethod(valueType)
            .Invoke(null, Array.empty)            

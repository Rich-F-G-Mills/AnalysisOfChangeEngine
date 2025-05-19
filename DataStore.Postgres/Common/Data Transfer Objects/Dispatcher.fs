
namespace AnalysisOfChangeEngine.DataStore.Postgres.DataTransferObjects

open System.Reflection
open FSharp.Linq.RuntimeHelpers
open FSharp.Reflection
open FSharp.Quotations

open Npgsql.FSharp

open AnalysisOfChangeEngine.DataStore.Postgres


(*
Design Decision:
    Why go through the hassle of writing a micro-ORM?
    Well... Why not? Ultimately, the queries we'll be running against the database will be very
    simple in construction. Through a combination of the DTO types and a liberal application of
    typed expressions, we can automate the generation of the SQL queries we'll need.

    The back-bone of all this is what is known as a 'dispatcher' object. This exposes a number of
    instance methods that allow us to query the database in a number of ways.
*)

module private RecordParser =

    // Need to wrap in module as cannot define directly within namespace.
    let internal create<'TRecord>
        (columns: PropertyInfo array, transferableTypes: ITransferableType array) : RowReader -> 'TRecord =
            let rowReaderVarDef =
                Var ("rowReader", typeof<RowReader>)

            let rowReaderVar =
                Expr.Var rowReaderVarDef
                // Cannot use downcast as this throws an exception at runtime.
                |> Expr.Cast<RowReader>

            let columnParsers =
                transferableTypes                
                |> Seq.zip columns
                |> Seq.map (fun (pi, tt) ->
                    tt.ReadSqlColumnExpr (rowReaderVar, pi.Name))
                |> Seq.toList

            let newDtoExpr =
                Expr.Lambda(rowReaderVarDef, Expr.NewRecord(typeof<'TRecord>, columnParsers))

            downcast LeafExpressionConverter.EvaluateQuotation newDtoExpr


type PostgresTableDispatcher<'TBaseRow, 'TAugRow> (tableName: string, schema: string, connection) =
    static let baseTableType =
        typeof<'TBaseRow>

    static let augTableType =
        typeof<'TAugRow>

    static let baseTableColumns =
        FSharpType.GetRecordFields (baseTableType, BindingFlags.NonPublic)

    static let augTableColumns =
        if augTableType = typeof<Unit> then
            Array.empty
        else
            FSharpType.GetRecordFields augTableType

    static let combinedTableColumns =
        Array.append baseTableColumns augTableColumns

    // As soon as we start depending on the schema or table name, we're no longer static.
    let baseTransferableTypes =
        baseTableColumns
        |> Array.map (_.PropertyType >> TransferableType.getForType schema)

    let augTransferableTypes =
        augTableColumns
        |> Array.map (_.PropertyType >> TransferableType.getForType schema)

    let combinedTransferableTypes =
        Array.append baseTransferableTypes augTransferableTypes

    let baseRecordParser: RowReader -> 'TBaseRow =
        RecordParser.create<_> (baseTableColumns, baseTransferableTypes)

    let augRecordParser: RowReader -> 'TAugRow =
        if augTableType = typeof<Unit> then
            fun _ -> Unchecked.defaultof<_>

        else
            RecordParser.create<_> (augTableColumns, augTransferableTypes)

    let combinedRecordParser reader =
        baseRecordParser reader, augRecordParser reader
            

    // Only includes the list of column names. Does NOT include SELECT or WHERE.
    // We wrap all column names in double quotes.
    let baseSqlColumnRefs =
        baseTransferableTypes
        |> Seq.zip baseTableColumns
        |> Seq.map (fun (pi, tt) ->
            tt.ToSqlSelector (sprintf "\"%s\"" pi.Name))
        |> String.join ", "

    let augSqlColumnRefs =
        augTransferableTypes
        |> Seq.zip augTableColumns
        |> Seq.map (fun (pi, tt) ->
            tt.ToSqlSelector (sprintf "\"%s\"" pi.Name))
        |> String.join ", "

    // Note that the schema and table name are wrapped in double quotes also.
    let baseSqlRecordSelector =
        sprintf "SELECT %s FROM \"%s\".\"%s\"" baseSqlColumnRefs schema tableName

    let augSqlRecordSelector =
        sprintf "SELECT %s FROM \"%s\".\"%s\"" augSqlColumnRefs schema tableName

    let combinedSqlRecordSelector =
        sprintf "SELECT %s, %s FROM \"%s\".\"%s\"" baseSqlColumnRefs augSqlColumnRefs schema tableName

    // Must be members in order to be generic w.r.t. filtered column type.
    // Note that we are ALWAYS filtering against the BASE columns,
    // regardless of what actually gets returned.
    member private _.makeEquality1Selector
        (column: Expr<'TBaseRow -> 'TValue>, sqlColumnSelector, parser) =
            let columnName =
                match column with
                | Patterns.Lambda (_, Patterns.PropertyGet (_, pi, _)) ->
                    pi.Name
                | _ ->
                    failwith "Unexpected pattern."

            let (tt: ITransferableType<'TValue>) =
                downcast TransferableType.getForType schema typeof<'TValue>

            let sqlWhere =
                sprintf "WHERE %s = %s"
                    (tt.ToSqlSelector (sprintf "\"%s\"" columnName))
                    (tt.ToSqlParamSite "@value")

            let sqlCommand =
                sprintf "%s %s" sqlColumnSelector sqlWhere

            fun value ->
                connection
                |> Sql.existingConnection
                |> Sql.query sqlCommand
                |> Sql.parameters [ "value", tt.ToSqlParamValue value ]
                |> Sql.execute parser

    member private _.makeEquality2Selector
        (column1: Expr<'TBaseRow -> 'TValue1>, column2: Expr<'TBaseRow -> 'TValue2>, sqlColumnSelector, parser) =
            let column1Name, column2Name =
                match column1, column2 with
                | Patterns.Lambda (_, Patterns.PropertyGet (_, pi1, _)),
                    Patterns.Lambda (_, Patterns.PropertyGet (_, pi2, _)) ->
                        pi1.Name, pi2.Name
                | _ ->
                    failwith "Unexpected pattern."

            let (tt1: ITransferableType<'TValue1>, tt2: ITransferableType<'TValue2>) =
                downcast TransferableType.getForType schema typeof<'TValue1>,
                downcast TransferableType.getForType schema typeof<'TValue2>

            let sqlWhere =
                sprintf "WHERE (%s = %s) AND (%s = %s)"
                    (tt1.ToSqlSelector (sprintf "\"%s\"" column1Name))
                    (tt1.ToSqlParamSite "@value1")
                    (tt2.ToSqlSelector (sprintf "\"%s\"" column2Name))
                    (tt2.ToSqlParamSite "@value2")

            let sqlCommand =
                sprintf "%s %s" sqlColumnSelector sqlWhere

            fun value1 value2 ->
                connection
                |> Sql.existingConnection
                |> Sql.query sqlCommand
                |> Sql.parameters [
                        "value1", tt1.ToSqlParamValue value1
                        "value2", tt2.ToSqlParamValue value2
                    ]
                |> Sql.execute parser

    member private _.makeEquality3Selector
        (column1: Expr<'TBaseRow -> 'TValue1>, column2: Expr<'TBaseRow -> 'TValue2>, column3: Expr<'TBaseRow -> 'TValue3>, sqlColumnSelector, parser) =
            let column1Name, column2Name, column3Name =
                match column1, column2, column3 with
                | Patterns.Lambda (_, Patterns.PropertyGet (_, pi1, _)),
                    Patterns.Lambda (_, Patterns.PropertyGet (_, pi2, _)),
                        Patterns.Lambda (_, Patterns.PropertyGet (_, pi3, _)) ->
                            pi1.Name, pi2.Name, pi3.Name
                | _ ->
                    failwith "Unexpected pattern."

            // Given we're not compiling code quotations, casting to the generic transferable types
            // provides us the type-safety we need.
            let (tt1: ITransferableType<'TValue1>, tt2: ITransferableType<'TValue2>, tt3: ITransferableType<'TValue3>) =
                downcast TransferableType.getForType schema typeof<'TValue1>,
                downcast TransferableType.getForType schema typeof<'TValue2>,
                downcast TransferableType.getForType schema typeof<'TValue3>

            let sqlWhere =
                sprintf "WHERE (%s = %s) AND (%s = %s) AND (%s = %s)"
                    (tt1.ToSqlSelector column1Name)
                    (tt1.ToSqlParamSite "@value1")
                    (tt2.ToSqlSelector column2Name)
                    (tt2.ToSqlParamSite "@value2")
                    (tt3.ToSqlSelector column3Name)
                    (tt3.ToSqlParamSite "@value3")

            let sqlCommand =
                sprintf "%s %s" sqlColumnSelector sqlWhere

            fun value1 value2 value3 ->
                connection
                |> Sql.existingConnection
                |> Sql.query sqlCommand
                |> Sql.parameters [
                        "value1", tt1.ToSqlParamValue value1
                        "value2", tt2.ToSqlParamValue value2
                        "value3", tt3.ToSqlParamValue value3
                    ]
                |> Sql.execute parser


    member this.MakeBaseEquality1Selector (column) =
        this.makeEquality1Selector (column, baseSqlRecordSelector, baseRecordParser)

    member this.MakeBaseEquality2Selector (column1, column2) =
        this.makeEquality2Selector (column1, column2, baseSqlRecordSelector, baseRecordParser)

    member this.MakeBaseEquality3Selector (column1, column2, column3) =
        this.makeEquality3Selector (column1, column2, column3, baseSqlRecordSelector, baseRecordParser)

    member this.MakeAugEquality1Selector (column) =
        this.makeEquality1Selector (column, augSqlRecordSelector, augRecordParser)

    member this.MakeAugEquality2Selector (column1, column2) =
        this.makeEquality2Selector (column1, column2, augSqlRecordSelector, augRecordParser)

    member this.MakeAugEquality3Selector (column1, column2, column3) =
        this.makeEquality3Selector (column1, column2, column3, augSqlRecordSelector, augRecordParser)

    member this.MakeCombinedEquality1Selector (column) =
        this.makeEquality1Selector (column, combinedSqlRecordSelector, combinedRecordParser)

    member this.MakeCombinedEquality2Selector (column1, column2) =
        this.makeEquality2Selector (column1, column2, combinedSqlRecordSelector, combinedRecordParser)

    member this.MakeCombinedEquality3Selector (column1, column2, column3) =
        this.makeEquality3Selector (column1, column2, column3, combinedSqlRecordSelector, combinedRecordParser)


    member _.SelectAllBaseRecords () =
        connection
        |> Sql.existingConnection
        |> Sql.query baseSqlRecordSelector
        |> Sql.execute baseRecordParser

    // Deferred so that we only create an inserter when required.
    member _.MakeRowInserter () =
        let combinedColumnNames =
            combinedTableColumns
            // We just need to list out the column names. This is NOT the same as
            // when we specify them for a SELECT statement.
            |> Seq.map (_.Name >> sprintf "\"%s\"")
            |> String.join ", "

        let paramNames =
            combinedTableColumns
            |> Array.indexed
            |> Array.map (fun (idx, _) -> sprintf "@value%i" idx)

        let combinedParamSites =
            paramNames                
            |> Seq.zip combinedTransferableTypes
            |> Seq.map (fun (tt, pn) -> tt.ToSqlParamSite pn)
            |> String.join ", "

        let sqlInsert =
            sprintf "INSERT INTO \"%s\".\"%s\" (%s) VALUES (%s)"
                schema tableName combinedColumnNames combinedParamSites

        let combinedRecordVarDef =
            Var ("record", typeof<'TBaseRow * 'TAugRow>)

        let combinedRecordVar =
            Expr.Var combinedRecordVarDef

        let combinedRecordVars =
            Array.append
                (Array.map (fun _ -> Expr.TupleGet (combinedRecordVar, 0)) baseTableColumns)
                (Array.map (fun _ -> Expr.TupleGet (combinedRecordVar, 1)) augTableColumns)                

        let paramValueTupleExprs =
            paramNames
            |> Seq.zip combinedRecordVars
            |> Seq.zip combinedTableColumns
            |> Seq.zip combinedTransferableTypes
            |> Seq.map (fun (tt, (pi, (var, pn))) ->
                Expr.NewTuple ([
                    Expr.Value pn
                    // We use Application here as we're calling a first-class value rather
                    // than a method.
                    Expr.Application (
                        tt.ToSqlParamValueExpr,
                        Expr.PropertyGet (var, pi))]))
            |> Seq.map Expr.Cast<string * SqlValue>

        let constructParamListExpr =
            Expr.Lambda(
                combinedRecordVarDef,
                // In theory, wouldn't matter whether we did a forward/back fold here.
                Seq.foldBack (fun e es -> <@ (%e)::(%es) @>) paramValueTupleExprs <@ [] @>
            )

        let (constructParamList: 'TBaseRow * 'TAugRow -> (string * SqlValue) list) =
            downcast LeafExpressionConverter.EvaluateQuotation constructParamListExpr

        fun row ->
            connection
            |> Sql.existingConnection
            |> Sql.query sqlInsert
            |> Sql.parameters (constructParamList row)
            |> Sql.executeNonQuery


[<Sealed>]
type PostgresCommonTableDispatcher<'TBaseRow, 'TAugRow> internal (tableName, connection) =
    inherit PostgresTableDispatcher<'TBaseRow, 'TAugRow> (tableName, "common", connection)

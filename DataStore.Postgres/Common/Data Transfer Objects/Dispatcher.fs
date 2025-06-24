
namespace AnalysisOfChangeEngine.DataStore.Postgres.DataTransferObjects

open System.Collections.Generic
open System.Data.Common
open System.Reflection
open FSharp.Linq.RuntimeHelpers
open FSharp.Reflection
open FSharp.Quotations

open Npgsql

open AnalysisOfChangeEngine.Common
open AnalysisOfChangeEngine.DataStore.Postgres


(*
Design Decision:
    Why go through the hassle of writing a micro-ORM?
    Well... Why not? Ultimately, the queries we'll be running against the database will be very
    simple in construction. Through a combination of the DTO types and a liberal application of
    typed expressions, we can automate the generation of the SQL queries we'll need.

    The back-bone of all this is what is known as a 'dispatcher' object. This exposes a number of
    instance methods that allow us to query the database in a number of (relatively) type-safe ways.

    In some cases, we are compiling code-quotations at runtime to achieve this.
*)

[<RequireQualifiedAccess>]
module internal RecordParser =

    // Need to wrap in module as cannot define directly within namespace.
    let internal Create<'TRecord>
        (transferableTypes: ITransferableType array, columnIdxs: int array)
        : DbDataReader  -> 'TRecord =
            let dataReaderVarDef =
                Var ("dataReader", typeof<DbDataReader>)

            let dataReaderVar =
                // Cannot use downcast as this throws an exception at runtime.
                Expr.Var dataReaderVarDef
                |> Expr.Cast<DbDataReader>

            let columnParsers =
                columnIdxs
                |> Array.zip transferableTypes
                |> Array.map (fun (tt, idx) ->
                    tt.ReadSqlColumnExpr (dataReaderVar, idx))
                |> Array.toList

            let newDtoExpr =
                Expr.Lambda(dataReaderVarDef, Expr.NewRecord(typeof<'TRecord>, columnParsers))

            downcast LeafExpressionConverter.EvaluateQuotation newDtoExpr


type PostgresTableDispatcher<'TBaseRow, 'TAugRow>
    (tableName: string, productSchema': string, dataSource: NpgsqlDataSource) =

        // --- HELPER DEFINITIONS ---

        static let baseTableType =
            typeof<'TBaseRow>

        static let augTableType =
            typeof<'TAugRow>

        static let baseTableColumns =
            FSharpType.GetRecordFields
                (baseTableType, BindingFlags.Public ||| BindingFlags.NonPublic)

        static let augTableColumns =
            if augTableType = typeof<Unit> then
                Array.empty
            else
                FSharpType.GetRecordFields augTableType

        static let combinedTableColumns =
            Array.append baseTableColumns augTableColumns   
        
        static let baseTransferableTypes =
            baseTableColumns
            |> Array.map (fun pi -> TransferableType.InvokeGetFor pi.PropertyType)

        static let augTransferableTypes =
            augTableColumns
            |> Array.map (fun pi -> TransferableType.InvokeGetFor pi.PropertyType)

        static let combinedTransferableTypes =
            Array.append baseTransferableTypes augTransferableTypes


        let productSchema =
            ProductSchemaName productSchema'


        let makeSelectors (tableColumns: PropertyInfo array, transferableTypes: ITransferableType array) =
            transferableTypes
            |> Array.zip tableColumns
            |> Array.map (fun (pi, tt) -> tt.SqlColMapper (sprintf "\"%s\"" pi.Name))

        let baseSqlColumnSelectors =
            makeSelectors (baseTableColumns, baseTransferableTypes)

        let augSqlColumnSelectors =
            makeSelectors (augTableColumns, augTransferableTypes)

        let combinedSqlColumnSelectors =
            makeSelectors (combinedTableColumns, combinedTransferableTypes)


        let joinedBaseSqlColumnSelectors =
            String.join ", " baseSqlColumnSelectors

        let joinedAugSqlColumnSelectors =
            String.join ", " augSqlColumnSelectors

        let joinedCombinedSqlColumnSelectors =
            String.join ", " combinedSqlColumnSelectors


        // Note that the schema and table name are wrapped in double quotes also.
        let baseSqlRecordSelector =
            sprintf "SELECT %s FROM \"%s\".\"%s\""
                joinedBaseSqlColumnSelectors productSchema' tableName

        let augSqlRecordSelector =
            sprintf "SELECT %s FROM \"%s\".\"%s\""
                joinedAugSqlColumnSelectors productSchema' tableName

        let combinedSqlRecordSelector =
            sprintf "SELECT %s FROM \"%s\".\"%s\""
                joinedCombinedSqlColumnSelectors productSchema' tableName


        let baseOnlyRecordParser =
            RecordParser.Create<'TBaseRow>
                (baseTransferableTypes, [| 0 .. baseTableColumns.Length - 1 |])

        let augOnlyRecordParser =
            if augTableType = typeof<Unit> then
                fun _ -> Unchecked.defaultof<'TAugRow>
            else
                RecordParser.Create<'TAugRow>
                    (augTransferableTypes, [| 0 .. augTableColumns.Length - 1 |])

        let combinedRecordParser =
            let augRecordParser' =
                if augTableType = typeof<Unit> then
                    fun _ -> Unchecked.defaultof<'TAugRow>
                else
                    // Need to make sure we skip the base columns.
                    RecordParser.Create<'TAugRow>
                        (augTransferableTypes, [| baseTableColumns.Length .. baseTableColumns.Length + augTableColumns.Length - 1 |])
            
            fun (dbReader: DbDataReader) ->
                let baseRecord =
                    baseOnlyRecordParser dbReader

                let augRecord =
                    augRecordParser' dbReader

                baseRecord, augRecord


        // Because we have both single and multiple row inserters... It seemed sensibible
        // to extract out common logic. Because we're using a let definition, it has to
        // appear before all member definitions.
        let sqlInsertRowCommand =
            let combinedColumnNames =
                    combinedTableColumns
                    // We just need to list out the column names. This is NOT the same as
                    // when we specify them for a SELECT statement.
                    |> Seq.map (_.Name >> sprintf "\"%s\"")
                    |> String.join ", "

            let paramIds =
                combinedTableColumns
                |> Array.mapi (fun idx _ -> $"${idx+1}")
                |> String.join ", "

            sprintf "INSERT INTO \"%s\".\"%s\" (%s) VALUES (%s)"
                productSchema' tableName combinedColumnNames paramIds


        // A helper function used in those instances where the developer can specific
        // a column to be used for filtering purposes.
        member private _.GetEqualitySelectorDetails (column: Expr<'TBaseRow -> 'TValue>) =
            let columnName =
                match column with
                | Patterns.Lambda (_, Patterns.PropertyGet (_, pi, _)) ->
                    pi.Name
                | _ ->
                    failwith "Unexpected pattern."

            let (tt: ITransferableType<'TValue>) =
                TransferableType.GetFor<_> ()

            let filterColSelector =
                tt.SqlColMapper (sprintf "\"%s\"" columnName)

            let toSqlParamValue =
                tt.ToSqlParamValue productSchema

            let toSqlParamValueArray =
                tt.ToSqlParamValueArray productSchema

            filterColSelector, toSqlParamValue, toSqlParamValueArray

        // Must be members in order to be generic w.r.t. filtered column type.
        // Note that we are ALWAYS filtering against the BASE columns,
        // regardless of what actually gets returned.
        member private this.MakeEquality1Selector
            // We need to specify the return type of 'parser' or the type inferencer will complain
            // when we're adding items to the linked-list.
            (column: Expr<'TBaseRow -> 'TValue>, sqlColumnsSelector, parser: DbDataReader -> 'TRow) =
                let filterColSelector, toSqlParamValue, _ =
                    this.GetEqualitySelectorDetails column

                let sqlCommand =
                    sprintf "%s WHERE %s = $1"
                        sqlColumnsSelector
                        filterColSelector

                let execute value =
                    use dbCommand =
                        dataSource.CreateCommand sqlCommand

                    do ignore <| dbCommand.Parameters.Add (toSqlParamValue value)
                    
                    use dbReader =
                        dbCommand.ExecuteReader ()

                    // We can't return a sequence as it's not clear how we'd ensure
                    // the reader would get disposed if we created it outside the sequence
                    // block. If we created within, same again!
                    [
                        while dbReader.Read () do
                            yield parser dbReader
                    ]

                let executeAsync value =
                    backgroundTask {
                        use dbCommand =
                            dataSource.CreateCommand sqlCommand

                        do ignore <| dbCommand.Parameters.Add (toSqlParamValue value)
                    
                        use! dbReader =
                            dbCommand.ExecuteReaderAsync ()

                        (*
                        Design Decision:
                            Why not just use a vanilla .NET list here?
                            We don't know hom many rows are going to be allocated. With this
                            approach, it'd be O(1) until capacity is reached, at which
                            point that next insertion becomes O(n).
                            Whereas, with the linked-list, we can enjoy O(1) insertions always.
                        *)
                        let rowsRead =
                            new LinkedList<_> ()

                        while! dbReader.ReadAsync () do
                            do ignore <| rowsRead.AddLast (parser dbReader)

                        (*
                        Design Decision:
                            No doubt we could do some recursive magic and output straight to an F# list.
                            However... Tasks don't support tail-calls. I suspect we'd end of switching
                            from the problem of transforming from one collection type to another, to instead
                            having to worry about blowing the stack if too many items are returned.
                            It's not obvious if there is a tail-recursive way of achieving this!
                        *)
                        return List.ofSeq rowsRead
                    }

                {|
                    Execute         = execute
                    ExecuteAsync    = executeAsync
                |}
                    

        member private this.MakeEquality2Selector
            (column1: Expr<'TBaseRow -> 'TValue1>, column2: Expr<'TBaseRow -> 'TValue2>, sqlColumnsSelector, parser: DbDataReader -> 'TRow) =
                let filterColSelector1, toSqlParamValue1, _ =
                    this.GetEqualitySelectorDetails column1

                let filterColSelector2, toSqlParamValue2, _ =
                    this.GetEqualitySelectorDetails column2

                let sqlCommand =
                    sprintf "%s WHERE (%s = $1) AND (%s = $2)"
                        sqlColumnsSelector
                        filterColSelector1
                        filterColSelector2

                let execute value1 value2 =
                    use dbCommand =
                        dataSource.CreateCommand sqlCommand

                    do ignore <| dbCommand.Parameters.Add (toSqlParamValue1 value1)
                    do ignore <| dbCommand.Parameters.Add (toSqlParamValue2 value2)
                    
                    use dbReader =
                        dbCommand.ExecuteReader ()

                    [
                        while dbReader.Read () do
                            yield parser dbReader
                    ]

                let executeAsync value1 value2 =
                    backgroundTask {
                        use dbCommand =
                            dataSource.CreateCommand sqlCommand

                        do ignore <| dbCommand.Parameters.Add (toSqlParamValue1 value1)
                        do ignore <| dbCommand.Parameters.Add (toSqlParamValue2 value2)
                    
                        use! dbReader =
                            dbCommand.ExecuteReaderAsync ()

                        let rowsRead =
                            new LinkedList<_> ()

                        while! dbReader.ReadAsync () do
                            do ignore <| rowsRead.AddLast (parser dbReader)

                        return List.ofSeq rowsRead
                    }

                {|
                    Execute         = execute
                    ExecuteAsync    = executeAsync
                |}

        member private this.MakeEquality3Selector
            (column1: Expr<'TBaseRow -> 'TValue1>, column2: Expr<'TBaseRow -> 'TValue2>, column3: Expr<'TBaseRow -> 'TValue3>, sqlColumnsSelector, parser: DbDataReader -> 'TRow) =
                let filterColSelector1, toSqlParamValue1, _ =
                    this.GetEqualitySelectorDetails column1

                let filterColSelector2, toSqlParamValue2, _ =
                    this.GetEqualitySelectorDetails column2

                let filterColSelector3, toSqlParamValue3, _ =
                    this.GetEqualitySelectorDetails column3

                let sqlCommand =
                    sprintf "%s WHERE (%s = $1) AND (%s = $2) AND (%s = $3)"
                        sqlColumnsSelector
                        filterColSelector1
                        filterColSelector2
                        filterColSelector3

                let execute value1 value2 value3 =
                    use dbCommand =
                        dataSource.CreateCommand sqlCommand

                    do ignore <| dbCommand.Parameters.Add (toSqlParamValue1 value1)
                    do ignore <| dbCommand.Parameters.Add (toSqlParamValue2 value2)
                    do ignore <| dbCommand.Parameters.Add (toSqlParamValue3 value3)
                    
                    use dbReader =
                        dbCommand.ExecuteReader ()

                    [
                        while dbReader.Read () do
                            yield parser dbReader
                    ]

                let executeAsync value1 value2 value3 =
                    backgroundTask {
                        use dbCommand =
                            dataSource.CreateCommand sqlCommand

                        do ignore <| dbCommand.Parameters.Add (toSqlParamValue1 value1)
                        do ignore <| dbCommand.Parameters.Add (toSqlParamValue2 value2)
                        do ignore <| dbCommand.Parameters.Add (toSqlParamValue3 value3)
                    
                        use! dbReader =
                            dbCommand.ExecuteReaderAsync ()

                        let rowsRead =
                            new LinkedList<_> ()

                        while! dbReader.ReadAsync () do
                            do ignore <| rowsRead.AddLast (parser dbReader)

                        return List.ofSeq rowsRead
                    }

                {|
                    Execute         = execute
                    ExecuteAsync    = executeAsync
                |}


        member this.MakeBaseEquality1Selector (column) =
            this.MakeEquality1Selector (column, baseSqlRecordSelector, baseOnlyRecordParser)

        member this.MakeBaseEquality2Selector (column1, column2) =
            this.MakeEquality2Selector (column1, column2, baseSqlRecordSelector, baseOnlyRecordParser)

        member this.MakeBaseEquality3Selector (column1, column2, column3) =
            this.MakeEquality3Selector (column1, column2, column3, baseSqlRecordSelector, baseOnlyRecordParser)

        member this.MakeAugEquality1Selector (column) =
            this.MakeEquality1Selector (column, augSqlRecordSelector, augOnlyRecordParser)

        member this.MakeAugEquality2Selector (column1, column2) =
            this.MakeEquality2Selector (column1, column2, augSqlRecordSelector, augOnlyRecordParser)

        member this.MakeAugEquality3Selector (column1, column2, column3) =
            this.MakeEquality3Selector (column1, column2, column3, augSqlRecordSelector, augOnlyRecordParser)

        member this.MakeCombinedEquality1Selector (column) =
            this.MakeEquality1Selector (column, combinedSqlRecordSelector, combinedRecordParser)

        member this.MakeCombinedEquality2Selector (column1, column2) =
            this.MakeEquality2Selector (column1, column2, combinedSqlRecordSelector, combinedRecordParser)

        member this.MakeCombinedEquality3Selector (column1, column2, column3) =
            this.MakeEquality3Selector (column1, column2, column3, combinedSqlRecordSelector, combinedRecordParser)


        member private this.MakeEquality1Multiple1Selector
            (column1: Expr<'TBaseRow -> 'TValue1>, column2: Expr<'TBaseRow -> 'TValue2>, sqlColumnsSelector, parser: DbDataReader -> 'TRow) =
                let filterColSelector1, toSqlParamValue1, _ =
                    this.GetEqualitySelectorDetails column1

                let filterColSelector2, _, toSqlParamValueArray2 =
                    this.GetEqualitySelectorDetails column2

                let sqlCommand =
                    sprintf "%s WHERE (%s = $1) AND (%s = ANY($2))"
                        sqlColumnsSelector
                        filterColSelector1
                        filterColSelector2

                let execute value1 values2 =
                    use dbCommand =
                        dataSource.CreateCommand sqlCommand

                    do ignore <| dbCommand.Parameters.Add (toSqlParamValue1 value1)
                    do ignore <| dbCommand.Parameters.Add (toSqlParamValueArray2 values2)
                    
                    use dbReader =
                        dbCommand.ExecuteReader ()
                    [
                        while dbReader.Read () do
                            yield parser dbReader
                    ]

                let executeAsync value1 values2 =
                    backgroundTask {
                        use dbCommand =
                            dataSource.CreateCommand sqlCommand

                        do ignore <| dbCommand.Parameters.Add (toSqlParamValue1 value1)
                        do ignore <| dbCommand.Parameters.Add (toSqlParamValueArray2 values2)
                    
                        use! dbReader =
                            dbCommand.ExecuteReaderAsync ()

                        let rowsRead =
                            new LinkedList<_> ()

                        while! dbReader.ReadAsync () do
                            do ignore <| rowsRead.AddLast (parser dbReader)

                        return  rowsRead
                    }

                {|
                    Execute         = execute
                    ExecuteAsync    = executeAsync
                |}

        member this.MakeCombinedEquality1Multiple1Selector (column1, column2) =
            this.MakeEquality1Multiple1Selector (column1, column2, combinedSqlRecordSelector, combinedRecordParser)


        member this.MakeEquality1Remover
            (column: Expr<'TBaseRow -> 'TValue>) =
                let filterColSelector, toSqlParamValue, _ =
                    this.GetEqualitySelectorDetails column

                let sqlCommand =
                    sprintf "DELETE FROM \"%s\".\"%s\" WHERE %s = $1"
                        productSchema'
                        tableName 
                        filterColSelector

                let prepareParameters (paramsCol: NpgsqlParameterCollection) value =
                    do ignore <| paramsCol.Add (toSqlParamValue value)

                let executeNonQuery value =
                    use dbCommand =
                        dataSource.CreateCommand sqlCommand

                    do prepareParameters dbCommand.Parameters value

                    dbCommand.ExecuteNonQuery ()

                let executeNonQueryAsync value =
                    backgroundTask {
                        use dbCommand =
                            dataSource.CreateCommand sqlCommand

                        do prepareParameters dbCommand.Parameters value

                        return! dbCommand.ExecuteNonQueryAsync ()
                    }                    

                let asBatchCommand value =
                    let dbBatchCommand = 
                        new NpgsqlBatchCommand (sqlCommand)

                    do ignore <| dbBatchCommand.Parameters.Add (toSqlParamValue value)

                    dbBatchCommand

                {|
                    ExecuteNonQuery         = executeNonQuery
                    ExecuteNonQueryAsync    = executeNonQueryAsync
                    AsBatchCommand          = asBatchCommand
                |}                

        member this.MakeEquality2Remover
            (column1: Expr<'TBaseRow -> 'TValue1>, column2: Expr<'TBaseRow -> 'TValue2>) =
                let filterColSelector1, toSqlParamValue1, _ =
                    this.GetEqualitySelectorDetails column1

                let filterColSelector2, toSqlParamValue2, _ =
                    this.GetEqualitySelectorDetails column2

                let sqlCommand =
                    sprintf "DELETE FROM \"%s\".\"%s\" WHERE (%s = $1) AND (%s = $2)"
                        productSchema'
                        tableName 
                        filterColSelector1
                        filterColSelector2

                let prepareParameters (paramsCol: NpgsqlParameterCollection) value1 value2 =
                    do ignore <| paramsCol.Add (toSqlParamValue1 value1)
                    do ignore <| paramsCol.Add (toSqlParamValue2 value2)

                let executeNonQuery value1 value2 =
                    use dbCommand =
                        dataSource.CreateCommand sqlCommand

                    do prepareParameters dbCommand.Parameters value1 value2

                    dbCommand.ExecuteNonQuery ()

                let asBatchCommand value1 value2 =
                    let dbBatchCommand = 
                        new NpgsqlBatchCommand (sqlCommand)

                    do prepareParameters dbBatchCommand.Parameters value1 value2

                    dbBatchCommand

                {|
                    ExecuteNonQuery = executeNonQuery
                    AsBatchCommand  = asBatchCommand
                |}

        member this.MakeEquality3Remover
            (column1: Expr<'TBaseRow -> 'TValue1>, column2: Expr<'TBaseRow -> 'TValue2>, column3: Expr<'TBaseRow -> 'TValue3>) =
                let filterColSelector1, toSqlParamValue1, _ =
                    this.GetEqualitySelectorDetails column1

                let filterColSelector2, toSqlParamValue2, _ =
                    this.GetEqualitySelectorDetails column2

                let filterColSelector3, toSqlParamValue3, _ =
                    this.GetEqualitySelectorDetails column3

                let sqlCommand =
                    sprintf "DELETE FROM \"%s\".\"%s\" WHERE (%s = $1) AND (%s = $2) AND (%s = $3)"
                        productSchema'
                        tableName 
                        filterColSelector1
                        filterColSelector2
                        filterColSelector3

                let prepareParameters (paramsCol: NpgsqlParameterCollection) value1 value2 value3 =
                    do ignore <| paramsCol.Add (toSqlParamValue1 value1)
                    do ignore <| paramsCol.Add (toSqlParamValue2 value2)
                    do ignore <| paramsCol.Add (toSqlParamValue3 value3)

                let executeNonQuery value1 value2 value3 =
                    use dbCommand =
                        dataSource.CreateCommand sqlCommand

                    do prepareParameters dbCommand.Parameters value1 value2 value3

                    dbCommand.ExecuteNonQuery ()

                let asBatchCommand value1 value2 value3 =
                    let dbBatchCommand = 
                        new NpgsqlBatchCommand (sqlCommand)

                    do prepareParameters dbBatchCommand.Parameters value1 value2 value3

                    dbBatchCommand

                {|
                    ExecuteNonQuery = executeNonQuery
                    AsBatchCommand  = asBatchCommand
                |}


        // In order to reduce the amount of heap allocated strings, 
        member this.MakeEquality1Multiple1Remover
            (column1: Expr<'TBaseRow -> 'TValue1>, column2: Expr<'TBaseRow -> 'TValue2>) =
                let filterColSelector1, toSqlParamValue1, _ =
                    this.GetEqualitySelectorDetails column1

                let filterColSelector2, _, toSqlParamValueArray2 =
                    this.GetEqualitySelectorDetails column2

                let sqlCommand =
                    sprintf "DELETE FROM \"%s\".\"%s\" WHERE (%s = $1) AND (%s = ANY($2))"
                        productSchema'
                        tableName
                        filterColSelector1
                        filterColSelector2

                let prepareParameters (paramsCol: NpgsqlParameterCollection) value1 values2 =
                    do ignore <| paramsCol.Add (toSqlParamValue1 value1)
                    do ignore <| paramsCol.Add (toSqlParamValueArray2 values2)

                let executeNonQuery value1 values2 =
                    use dbCommand =
                        dataSource.CreateCommand sqlCommand

                    do prepareParameters dbCommand.Parameters value1 values2

                    dbCommand.ExecuteNonQuery ()

                let executeNonQueryAsync value1 values2 =
                    backgroundTask {
                        use dbCommand =
                            dataSource.CreateCommand sqlCommand

                        do prepareParameters dbCommand.Parameters value1 values2

                        return! dbCommand.ExecuteNonQueryAsync ()
                    }                    

                let asBatchCommand value1 values2 =
                    let dbBatchCommand = 
                        new NpgsqlBatchCommand (sqlCommand)

                    do prepareParameters dbBatchCommand.Parameters value1 values2

                    dbBatchCommand

                {|
                    ExecuteNonQuery         = executeNonQuery
                    ExecuteNonQueryAsync    = executeNonQueryAsync
                    AsBatchCommand          = asBatchCommand
                |}


        member _.SelectAllBaseRecords () =
            use dbCommand =
                dataSource.CreateCommand baseSqlRecordSelector

            use dbReader =
                dbCommand.ExecuteReader ()

            [
                while dbReader.Read () do
                    yield baseOnlyRecordParser dbReader
            ]


        (*
        Design decision:
            Why go to all this trouble? Why not just create this upfront regardless?
            If nothing else, we'd could end up creating logic for tables that will never been updated.
            This wasn't a performance decision, but a philosophical one. Furthermore,
            given these same outputs would be used for both single and multiple row insertions,
            it made sense to extract out this common logic and only instantiate when needed.
        *)
        member val private getRowInsertParameters =
            lazy (
                let combinedRecordVarDef =
                    Var ("record", typeof<'TBaseRow * 'TAugRow>)

                let combinedRecordVar =
                    Expr.Var combinedRecordVarDef

                let combinedRecordVars =
                    Array.append
                        (Array.map (fun _ -> Expr.TupleGet (combinedRecordVar, 0)) baseTableColumns)
                        (Array.map (fun _ -> Expr.TupleGet (combinedRecordVar, 1)) augTableColumns)                

                let paramValueTupleExprs =
                    combinedRecordVars
                    |> Seq.zip combinedTableColumns
                    |> Seq.zip combinedTransferableTypes
                    |> Seq.map (fun (tt, (pi, var)) ->
                        Expr.Application (
                            // Other than the fact that these are injected during start-up.
                            // the current implementation also caches the resulting expression.
                            tt.ToSqlParamValueExpr productSchema,
                            Expr.PropertyGet (var, pi)
                        ))
                    |> Seq.map Expr.Cast<NpgsqlParameter>

                let constructParamListExpr =
                    Expr.Lambda(
                        combinedRecordVarDef,
                        // In order for the resulting parameters to be in the right order,
                        // we use a reverse fold here.
                        Seq.foldBack (fun e es -> <@ (%e)::(%es) @>) paramValueTupleExprs <@ [] @>
                    )

                let (constructParamList: 'TBaseRow * 'TAugRow -> NpgsqlParameter list) =
                    downcast LeafExpressionConverter.EvaluateQuotation constructParamListExpr

                constructParamList
            ) with get

        member this.MakeRowInserter () =
            let prepareParameters (paramsCol: NpgsqlParameterCollection) row =
                let rowParams: NpgsqlParameter list =
                    this.getRowInsertParameters.Value row

                for rowParam in rowParams do
                    do ignore <| paramsCol.Add rowParam

            let executeNonQuery row =
                use dbCommand =
                    dataSource.CreateCommand sqlInsertRowCommand

                do prepareParameters dbCommand.Parameters row

                dbCommand.ExecuteNonQuery ()

            let executeNonQueryAsync row =
                backgroundTask {
                    use dbCommand =
                        dataSource.CreateCommand sqlInsertRowCommand

                    do prepareParameters dbCommand.Parameters row

                    return! dbCommand.ExecuteNonQueryAsync ()
                }                

            let asBatchCommand row =
                let dbBatchCommand = 
                    new NpgsqlBatchCommand (sqlInsertRowCommand)

                let rowParams =
                    this.getRowInsertParameters.Value row

                for rowParam in rowParams do
                    do ignore <| dbBatchCommand.Parameters.Add rowParam

                dbBatchCommand

            {|
                ExecuteNonQuery         = executeNonQuery
                ExecuteNonQueryAsync    = executeNonQueryAsync
                AsBatchCommand          = asBatchCommand
            |}

        member this.MakeRowsInserter () =
            let prepareBatchCommand (dbBatch: NpgsqlBatch) rows =
                for row in rows do
                    // Doesn't implement IDisposable so no need for 'use' here.
                    let dbBatchCommand =
                        dbBatch.CreateBatchCommand (CommandText = sqlInsertRowCommand)

                    let rowParams =
                        this.getRowInsertParameters.Value row

                    for rowParam in rowParams do
                        do ignore <| dbBatchCommand.Parameters.Add rowParam

            let executeNonQuery (rows: _ list) =
                use dbBatch =
                    dataSource.CreateBatch ()

                do prepareBatchCommand dbBatch rows

                dbBatch.ExecuteNonQuery ()

            let executeNonQueryAsync (rows: _ list) =
                backgroundTask {
                    use dbBatch =
                        dataSource.CreateBatch ()

                    do prepareBatchCommand dbBatch rows

                    return! dbBatch.ExecuteNonQueryAsync ()
                }
                
            let asBatchCommands rows =
                rows |>
                List.map (fun row ->
                    let dbBatchCommand = 
                        new NpgsqlBatchCommand (sqlInsertRowCommand)

                    let rowParams =
                        this.getRowInsertParameters.Value row

                    for rowParam in rowParams do
                        do ignore <| dbBatchCommand.Parameters.Add rowParam

                    dbBatchCommand
                )                

            {|
                ExecuteNonQuery         = executeNonQuery
                executeNonQueryAsync    = executeNonQueryAsync
                AsBatchCommand          = asBatchCommands
            |}


[<Sealed>]
// By definition, it wouldn't make sense for this to be used outside of this assembly.
type internal PostgresCommonTableDispatcher<'TBaseRow, 'TAugRow> internal (tableName, dataSource) =
    inherit PostgresTableDispatcher<'TBaseRow, 'TAugRow>
        (tableName, "common", dataSource)

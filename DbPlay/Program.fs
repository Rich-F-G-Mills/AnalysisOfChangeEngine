

open Npgsql
open System


[<EntryPoint>]
let main _ =
    let connStr =
        NpgsqlConnectionStringBuilder(
            Host = "localhost",
            Port = 5432,
            Database = "analysis_of_change",
            Username = "postgres",
            Password = "internet"
        )   

    use dataSource =
        NpgsqlDataSource.Create connStr.ConnectionString

    let param1 =
        new NpgsqlParameter<string>(
            TypedValue = "WARNING",
            DataTypeName = "common.validation_issue_classification"
        )

    let param2 =
        new NpgsqlParameter<string>(
            TypedValue = "ERROR",
            DataTypeName = "common.validation_issue_classification"
        )

    let nullParam =
        new NpgsqlParameter(Value = DBNull.Value)

    use dbBatch =
        dataSource.CreateBatch ()

    let b1 =
        dbBatch.CreateBatchCommand ()

    do b1.CommandText <- "INSERT INTO ob_wol.test(x) VALUES ($1)"

    do ignore <| b1.Parameters.Add param1

    let b2 =
        dbBatch.CreateBatchCommand ()

    do b2.CommandText <- "INSERT INTO ob_wol.test(x) VALUES ($1)"

    do ignore <| b2.Parameters.Add param2

    do dbBatch.BatchCommands.Add b1
    do dbBatch.BatchCommands.Add b2

    do ignore <| dbBatch.ExecuteNonQuery()

    0
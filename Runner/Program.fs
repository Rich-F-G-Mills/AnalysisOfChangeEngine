
namespace AnalysisOfChangeEngine


module Runner =

    open FsToolkit.ErrorHandling
    open Npgsql
    open Npgsql.FSharp
    open AnalysisOfChangeEngine
    open AnalysisOfChangeEngine.Controller
    open AnalysisOfChangeEngine.Controller.DataStore


    let logger =
        {
            new ILogger with
                member _.LogDebug message =
                    do printfn "DEBUG: %s" message

                member _.LogError message = 
                    do printfn "ERROR: %s" message

                member _.LogInfo message = 
                    do printfn "INFO: %s" message

                member _.LogWarning message = 
                    do printfn "WARNING: %s" message
        }


    [<EntryPoint>]
    let main _ =
        result {
            let sessionContext: SessionContext =
                { UserName = "RICH" }

            let connStr =
                Sql.host "localhost"
                |> Sql.port 5432
                |> Sql.database "analysis_of_change"
                |> Sql.username "postgres"
                |> Sql.password "internet"
                |> Sql.formatConnectionString

            use connection =
                new NpgsqlConnection (connStr)

            let dataStore =
                new Postgres.DataStore (sessionContext, connection)

            //let newProduct =
            //    dataStore.CreateProduct ("OB Whole-Life", "LVFS CWP OB Whole-Life")

            let products =
                dataStore.GetAllProducts ()

            do printfn "Test: %A" products

            return 0
        }
        |> Result.teeError (printfn "Error: %s")
        |> Result.defaultValue -1
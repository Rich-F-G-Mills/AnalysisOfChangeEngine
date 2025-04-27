
namespace AnalysisOfChangeEngine.DataStore


[<RequireQualifiedAccess>]
module Postgres =

    [<RequireQualifiedAccess>]
    module OBWholeOfLife =

        open Npgsql

        type DataStore (sessionContext: Postgres.SessionContext, connection: NpgsqlConnection) =
            inherit Postgres.AbstractDataStore (connection, "ob_wol")


namespace AnalysisOfChangeEngine.Controller.DataStore


open System
open FsToolkit.ErrorHandling
open Npgsql
open Npgsql.FSharp
open AnalysisOfChangeEngine.Controller


[<NoEquality; NoComparison>]
type Product =
    {
        Uid                         : Guid
        Name                        : string
        Description                 : string
        CreatedBy                   : string
        CreatedWhen                 : DateTime
    }


[<NoEquality; NoComparison>]
type RunHeader =
    {
        Uid                         : Guid
        Title                       : string
        Comments                    : string option
        CreatedBy                   : string
        CreatedWhen                 : DateTime
        OpeningRunUid               : Guid option
        ClosingRunDate              : DateOnly
        ProductUid                  : Guid
        PolicyDataTableOid          : uint32
        PolicyDataTableName         : string
        PolicyDataExtractionUid     : Guid        
    }


type PostgresDataStore (sessionContext: SessionContext, connection: NpgsqlConnection) =

    static let [<Literal>] productSql =
        "SELECT * FROM products"

    static let [<Literal>] runHeaderSql =
        "SELECT *, pgc.relname
         FROM run_headers AS rh
         LEFT JOIN pg_catalog.pg_class AS pgc
         ON pgc.oid = rh.policy_data_oid"

    static let parseProductRow (r: RowReader) =
        {
            Uid = r.uuid "uid"
            Name = r.text "name"
            Description = r.text "description"
            CreatedBy = r.text "created_by"
            CreatedWhen = r.dateTime "created_when"
        }

    static let parseRunHeader (r: RowReader) =
        {
            Uid = r.uuid "uid"
            Title = r.text "title"
            Comments = r.textOrNone "comments"
            CreatedBy = r.text "created_by"
            CreatedWhen = r.dateTime "created_when"
            OpeningRunUid = r.uuidOrNone "opening_run_uid"
            ClosingRunDate = r.dateOnly "closing_run_date"
            ProductUid = r.uuid "product_uid"
            PolicyDataTableOid = BitConverter.ToUInt32 (r.bytea "policy_data_oid")
            PolicyDataTableName = r.text "policy_data_table_name"
            PolicyDataExtractionUid = r.uuid "policy_data_extraction_uid"
        }


    member _.GetProduct (uid: Guid) =
        connection
        |> Sql.existingConnection
        |> Sql.query (sprintf "%s WHERE uid = '%A'" productSql uid)
        |> Sql.executeRow parseProductRow

    member _.GetAllProducts () =
        connection
        |> Sql.existingConnection
        |> Sql.query productSql
        |> Sql.execute parseProductRow

    member _.GetRunHeader (uid: Guid) =
        connection
        |> Sql.existingConnection
        |> Sql.query (sprintf "%s WHERE uid = '%A'" runHeaderSql uid)
        |> Sql.executeRow parseRunHeader

    member _.GetAllRunHeaders () =
        connection
        |> Sql.existingConnection
        |> Sql.query runHeaderSql
        |> Sql.execute parseRunHeader


    member _.CreateProduct (name, description, ?uid) =
        let newProduct: Product =
            {
                Uid = defaultArg uid (Guid.NewGuid ())
                Name = name
                Description = description
                CreatedBy = sessionContext.UserName
                CreatedWhen = DateTime.Now
            }

        connection
        |> Sql.existingConnection
        |> Sql.query "INSERT INTO products (
	        uid, name, description, created_by, created_when)
            VALUES (@uid, @name, @description, @created_by, @created_when)"
        |> Sql.parameters
            [
                "uid", SqlValue.Uuid newProduct.Uid
                "name", SqlValue.String newProduct.Name
                "description", SqlValue.String newProduct.Description
                "created_by", SqlValue.String newProduct.CreatedBy
                "created_when", SqlValue.Timestamp newProduct.CreatedWhen
            ]
        |> Sql.executeNonQuery
        |> ignore

        newProduct
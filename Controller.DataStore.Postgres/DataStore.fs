
namespace AnalysisOfChangeEngine.Controller.DataStore


open System
open FsToolkit.ErrorHandling
open FSharp.Data.Sql
open Npgsql
open Npgsql.FSharp
open AnalysisOfChangeEngine.Controller


[<RequireQualifiedAccess>]
module Postgres =

    let [<Literal>] private resolutionPath =
        @"C:\Users\Millch\Documents\AnalysisOfChangeEngine\Controller.DataStore.Postgres"


    type private DbSchema =
        SqlDataProvider<
            DatabaseVendor=Common.DatabaseProviderTypes.POSTGRESQL,
            ConnectionString="Host=localhost;Port=5432;Database=analysis_of_change;Username=postgres;Password=internet",
            UseOptionTypes=Common.NullableColumnType.OPTION,
            ResolutionPath=resolutionPath>


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
            PolicyDataExtractionUid     : Guid        
        }


    let private parseProductRow (row: DbSchema.dataContext.``public.productsEntity``) =
        {
            Uid = row.Uid
            Name = row.Name
            Description = row.Description
            CreatedBy = row.CreatedBy
            CreatedWhen = row.CreatedWhen
        }

    let private parseRunHeaderRow (row: DbSchema.dataContext.``public.run_headersEntity``) =
        {
            Uid = row.Uid
            Title = row.Title
            Comments = row.Comments
            ProductUid = row.ProductUid
            CreatedBy = row.CreatedBy
            CreatedWhen = row.CreatedWhen
            OpeningRunUid = row.OpeningRunUid
            ClosingRunDate = DateOnly.FromDateTime row.ClosingRunDate
            PolicyDataTableOid = row.PolicyDataTableOid
            PolicyDataExtractionUid = row.PolicyDataExtractionUid
        }


    type DataStore (sessionContext: SessionContext, connection: NpgsqlConnection) =

        let dbContext =
            DbSchema.GetDataContext (connection.ConnectionString)        

        member _.GetProduct (uid: Guid) =
            query {
                for product in dbContext.Public.Products do
                    where (product.Uid = uid)
                    select (product)
            }
            |> Seq.map parseProductRow
            |> Seq.toList

        member _.GetAllProducts () =
            dbContext.Public.Products
            |> Seq.map parseProductRow
            |> Seq.toList

        member _.GetRunHeader (uid: Guid) =
            query {
                for runHdr in dbContext.Public.RunHeaders do
                    where (runHdr.Uid = uid)
                    select runHdr
            }
            |> Seq.map parseRunHeaderRow
            |> Seq.toList

        member _.GetAllRunHeaders () =
            dbContext.Public.RunHeaders
            |> Seq.map parseRunHeaderRow
            |> Seq.toList

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
            |> Sql.query 
                "INSERT INTO products (uid, name, description, created_by, created_when)
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
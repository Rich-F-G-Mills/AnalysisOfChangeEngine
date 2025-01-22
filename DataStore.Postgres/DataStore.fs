 
namespace AnalysisOfChangeEngine.DataStore


open System
open Npgsql
open Npgsql.FSharp
open AnalysisOfChangeEngine


[<RequireQualifiedAccess>]
module Postgres =
    
    [<NoEquality; NoComparison>]
    type SessionContext =
        {
            UserName    : string
        }


    [<NoEquality; NoComparison>]
    type Product =
        {
            Uid                         : Guid
            Name                        : string
            Description                 : string
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
            PolicyDataExtractionUid     : Guid        
        }

    [<NoEquality; NoComparison>]
    type StepHeader =
        {
            Uid                         : Guid
            Title                       : string
            Description                 : string    
        }


    let private parseProductRow (row: RowReader)
        : Product =
            {
                Uid =
                    row.uuid "uid"
                Name =
                    row.text "name"
                Description =
                    row.text "description"
            }

    let private parseRunHeaderRow (row: RowReader)
        : RunHeader =
            {
                Uid =
                    row.uuid "uid"
                Title =
                    row.text "title"
                Comments =
                    row.textOrNone "comments"
                ProductUid =
                    row.uuid "product_uid"
                CreatedBy =
                    row.text "created_by"
                CreatedWhen =
                    row.dateTime "created_when"
                OpeningRunUid =
                    row.uuidOrNone "opening_run_uid"
                ClosingRunDate =
                    row.dateOnly "closing_run_date"
                PolicyDataExtractionUid =
                    row.uuid "policy_data_extraction_uid"
            }

    let private parseStepHeaderRow (row: RowReader)
        : StepHeader =
            {
                Uid =
                    row.uuid "uid"
                Title =
                    row.text "title"
                Description =
                    row.text "description"         
            }


    type DataStore (sessionContext: SessionContext, connection: NpgsqlConnection) =

        member this.CreateUidResolver () =
            let stepHeaders =
                this.GetAllStepHeaders ()
                |> Seq.map (fun sh -> sh.Uid, sh)
                |> Map.ofSeq

            fun uid ->
                let stepHeader =
                    stepHeaders[uid]

                stepHeader.Title, stepHeader.Description

        member _.TryGetProduct (uid: Guid) =
            connection
            |> Sql.existingConnection
            |> Sql.query "SELECT * FROM products WHERE uid = @uid"
            |> Sql.parameters [ "uid", SqlValue.Uuid uid ]
            |> Sql.execute parseProductRow
            |> List.tryExactlyOne

        member _.GetAllProducts () =
            connection
            |> Sql.existingConnection
            |> Sql.query "SELECT * FROM products"
            |> Sql.execute parseProductRow

        member _.TryGetRunHeader (uid: Guid) =
            connection
            |> Sql.existingConnection
            |> Sql.query "SELECT * FROM run_headers WHERE uid = @uid"
            |> Sql.parameters [ "uid", SqlValue.Uuid uid ] 
            |> Sql.execute parseRunHeaderRow
            |> List.tryExactlyOne

        member _.GetAllRunHeaders () =
            connection
            |> Sql.existingConnection
            |> Sql.query "SELECT * FROM run_headers"
            |> Sql.execute parseRunHeaderRow

        member _.GetAllStepHeaders () =
            connection
            |> Sql.existingConnection
            |> Sql.query "SELECT * FROM step_headers"
            |> Sql.execute parseStepHeaderRow

        member _.CreateProduct (name, description, ?uid) =
            let newProduct: Product =
                {
                    Uid = defaultArg uid (Guid.NewGuid ())
                    Name = name
                    Description = description
                }

            connection
            |> Sql.existingConnection
            |> Sql.query 
                "INSERT INTO products (uid, name, description)
                 VALUES (@uid, @name, @description)"
            |> Sql.parameters
                [
                    "uid", SqlValue.Uuid newProduct.Uid
                    "name", SqlValue.String newProduct.Name
                    "description", SqlValue.String newProduct.Description
                ]
            |> Sql.executeNonQuery
            |> ignore

            newProduct
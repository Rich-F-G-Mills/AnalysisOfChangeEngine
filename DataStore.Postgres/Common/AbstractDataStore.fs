 
namespace AnalysisOfChangeEngine.DataStore


[<RequireQualifiedAccess>]
module Postgres =

    open System
    open Npgsql
    open Npgsql.FSharp

    
    [<NoEquality; NoComparison>]
    type SessionContext =
        {
            UserName    : string
        }

    [<NoEquality; NoComparison>]
    type ExtractionHeader =
        {
            Uid                         : Guid
            ExtractionDate              : DateTime
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
        

    let private parseExtractionHeaderRow (row: RowReader)
        : ExtractionHeader =
            {
                Uid =
                    row.uuid "uid"
                ExtractionDate =
                    row.dateTime "extraction_date"
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


    [<AbstractClass>]
    type AbstractDataStore (connection: NpgsqlConnection, schema: string) =

        // --- HELPERS ---

        let getAllFromTable tableName rowProcessor =
            connection
            |> Sql.existingConnection
            |> Sql.query $"SELECT * FROM {schema}.{tableName}"
            |> Sql.execute rowProcessor

        let tryGetFromTable tableName uid rowProcessor =
            connection
            |> Sql.existingConnection
            |> Sql.query $"SELECT * FROM {schema}.{tableName} WHERE uid = @uid"
            |> Sql.parameters [ "uid", SqlValue.Uuid uid ]
            |> Sql.execute rowProcessor
            |> List.tryExactlyOne


        // --- UID RESOLVER ---

        member this.CreateUidResolver () =
            let stepHeaders =
                this.GetAllStepHeaders ()
                |> Seq.map (fun sh -> sh.Uid, sh)
                |> Map.ofSeq

            fun uid ->
                let stepHeader =
                    stepHeaders[uid]

                stepHeader.Title, stepHeader.Description


        // --- EXTRACTION HEADERS ---

        member _.TryGetExtractionHeaders (uid: Guid) =
            tryGetFromTable "extraction_headers" uid parseExtractionHeaderRow

        member _.GetAllExtractionHeaders () =
            getAllFromTable "extraction_headers" parseExtractionHeaderRow


        // --- RUN HEADERS ---

        member _.TryGetRunHeader (uid: Guid) =
            tryGetFromTable "run_headers" uid parseRunHeaderRow

        member _.GetAllRunHeaders () =
            getAllFromTable "run_headers" parseRunHeaderRow


        // --- STEP HEADERS ---

        member _.GetAllStepHeaders () =
            connection
            |> Sql.existingConnection
            |> Sql.query $"SELECT * FROM common.step_headers"
            |> Sql.execute parseStepHeaderRow
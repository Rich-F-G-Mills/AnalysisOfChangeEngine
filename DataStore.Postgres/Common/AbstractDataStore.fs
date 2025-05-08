 
namespace AnalysisOfChangeEngine.DataStore.Postgres


[<AutoOpen>]
module AbstractDataStore =

    open System
    open System.Text
    open Microsoft.Extensions.ObjectPool
    open Npgsql
    open Npgsql.FSharp
    open FsToolkit.ErrorHandling
    open AnalysisOfChangeEngine
        

    let private parseExtractionHeaderRow (row: RowReader)
        : ExtractionHeader =
            {
                Uid =
                    ExtractionUid (row.uuid "uid")
                ExtractionDate =
                    row.dateOnly "extraction_date"
            }

    let private parseRunHeaderRow (row: RowReader)
        : RunHeader =
            {
                Uid =
                    RunUid (row.uuid "uid")
                Title =
                    row.text "title"
                Comments =
                    row.textOrNone "comments"
                CreatedBy =
                    row.text "created_by"
                CreatedWhen =
                    row.dateTime "created_when"
                OpeningRunUid =
                    row.uuidOrNone "opening_run_uid"
                    |> Option.map RunUid
                ClosingRunDate =
                    row.dateOnly "closing_run_date"
                PolicyDataExtractionUid =
                    ExtractionUid (row.uuid "policy_data_extraction_uid")                    
            }

    let private parseStepHeaderRow (row: RowReader)
        : StepHeader =
            {
                Uid =
                    StepUid (row.uuid "uid")
                Title =
                    row.text "title"
                Description =
                    row.text "description"         
            }


    [<AbstractClass>]
    type AbstractDataStore<'TPolicyRecord, 'TPolicyRecordDto, 'TStepResults, 'TStepResultsDto>
        (sessionContext: SessionContext, connection: NpgsqlConnection, schema: string) =

        // Looking at https://github.com/dotnet/aspnetcore/blob/d12915f18974ae45826ac7475c5c87aaef218615/src/ObjectPool/src/StringBuilderPooledObjectPolicy.cs#L41...
        // ...we can see that the string builder is cleared when it is returned to the pool.
        static let stringBuilderPool =
            (new DefaultObjectPoolProvider()).CreateStringBuilderPool()


        // --- HELPERS ---

        let getAllFromTable tableName rowProcessor =
            connection
            |> Sql.existingConnection
            |> Sql.query $"SELECT * FROM {schema}.{tableName}"
            |> Sql.execute rowProcessor

        let tryGetFromTableUsingUid tableName uid rowProcessor =
            connection
            |> Sql.existingConnection
            |> Sql.query $"SELECT * FROM {schema}.{tableName} WHERE uid = @uid"
            |> Sql.parameters [ "uid", SqlValue.Uuid uid ]
            |> Sql.execute rowProcessor
            |> function
                | [] ->
                    None
                | [ row ] ->
                    Some row
                | _ ->
                    // Although not (necessarily) the developers fault, this would need to be investigated.
                    failwithf "Multiple rows found for UID %A in table %s.%s" uid schema tableName


        // --- UID RESOLVER ---

        member this.CreateStepUidResolver () =
            let stepHeaders =
                this.GetAllStepHeaders ()
                |> Seq.map (fun sh -> sh.Uid.Value, sh)
                |> Map.ofSeq

            fun uid ->
                let stepHeader =
                    stepHeaders[uid]

                stepHeader.Title, stepHeader.Description


        // --- EXTRACTION HEADERS ---

        member _.CreateExtractionHeader (extractionDate: DateOnly) =
            let newUid =
                Guid.NewGuid ()

            let newRow =
                {
                    Uid = ExtractionUid newUid
                    ExtractionDate = extractionDate
                }

            connection
            |> Sql.existingConnection
            |> Sql.query $"INSERT INTO {schema}.extraction_headers (uid, extraction_date) VALUES (@uid, @extraction_date)"
            |> Sql.parameters [
                    "uid", SqlValue.Uuid newUid
                    "extraction_date", SqlValue.Date (Choice2Of2 newRow.ExtractionDate)
                ]
            |> Sql.executeNonQuery
            |> ignore
            
            newRow

        member _.TryGetExtractionHeader (ExtractionUid runUid') =
            tryGetFromTableUsingUid "extraction_headers" runUid' parseExtractionHeaderRow

        member _.GetAllExtractionHeaders () =
            getAllFromTable "extraction_headers" parseExtractionHeaderRow


        // --- POLICY DATA ---

        abstract member dtoToPolicyRecord: 'TPolicyRecordDto -> Result<'TPolicyRecord, string>

        abstract member policyRecordToDto: 'TPolicyRecord -> Result<'TPolicyRecordDto, string>


        // --- RUNS ---

        member _.TryGetRunHeader (RunUid runUid') =
            tryGetFromTableUsingUid "run_headers" runUid' parseRunHeaderRow

        member _.GetAllRunHeaders () =
            getAllFromTable "run_headers" parseRunHeaderRow

        member this.CreateRun (title, comments, openingRunUid, closingRunDate, policyDataExtractionUid, walk: #AbstractWalk<_, _, _>) =
            let stepHeaders =
                this.GetAllStepHeaders ()
                |> Seq.map (fun hdr -> hdr.Uid.Value)
                |> Set

            let allWalkSteps =
                walk.AllSteps
                |> Seq.toList

            do  allWalkSteps
                |> Seq.map _.Uid
                |> Seq.tryFind (fun stepUid' -> not (Set.contains stepUid' stepHeaders))
                |> function
                    // This would be a very silly developer error and they should be punished accordingly!
                    | Some uid  -> failwithf "Step UID %A not found in the database." uid
                    | None      -> ()

            let newUid =
                Guid.NewGuid ()

            let newRow =
                {
                    Uid                     = RunUid newUid
                    Title                   = title
                    Comments                = comments
                    CreatedBy               = sessionContext.UserName
                    CreatedWhen             = DateTime.Now
                    OpeningRunUid           = openingRunUid
                    ClosingRunDate          = closingRunDate
                    PolicyDataExtractionUid = policyDataExtractionUid
                }

            let openingRunUid' =
                openingRunUid
                |> Option.map _.Value

            // TODO - Could this all be done as part of the same transaction?

            do  connection
                |> Sql.existingConnection
                |> Sql.query
                    $"""INSERT INTO {schema}.run_headers (uid, title, comments, created_by, created_when, opening_run_uid, closing_run_date, policy_data_extraction_uid)
                        VALUES (@uid, @title, @comments, @created_by, @created_when, @opening_run_uid, @closing_run_date, @policy_data_extraction_uid)"""
                |> Sql.parameters [
                        "uid", SqlValue.Uuid newUid
                        "title", SqlValue.String newRow.Title
                        "comments", SqlValue.StringOrNull newRow.Comments
                        "created_by", SqlValue.String newRow.CreatedBy
                        "created_when", SqlValue.Timestamp newRow.CreatedWhen
                        "opening_run_uid", SqlValue.UuidOrNull openingRunUid'
                        "closing_run_date", SqlValue.Date (Choice2Of2 newRow.ClosingRunDate)
                        "policy_data_extraction_uid", SqlValue.Uuid policyDataExtractionUid.Value
                    ]
                |> Sql.executeNonQuery
                |> ignore

            let stepUidsSql =
                String.Join(
                    ", ", 
                    allWalkSteps
                    |> Seq.indexed
                    |> Seq.map (fun (idx, hdr: IStepHeader) ->
                        sprintf "('%A', %i, '%A')" newRow.Uid.Value idx hdr.Uid) 
                )

            do  connection
                |> Sql.existingConnection
                |> Sql.query
                    $"INSERT INTO {schema}.run_steps (run_uid, step_idx, step_uid) VALUES {stepUidsSql}"
                |> Sql.executeNonQuery
                |> ignore

            newRow

        member this.CreateStepResultsWriter (RunUid runUid' as runUid) =
            result {
                let! stepResultHeadersMap =
                    this.TryGetStepHeadersForRun runUid
                    |> Result.requireSome (sprintf "Unable to locate run header for UID %A." runUid')

                return fun (results: Map<Guid, 'TStepResults>) ->
                    let sb =
                        stringBuilderPool.Get ()

                    do sb.Append $"INSERT INTO {schema}.step_results ("

                    do stringBuilderPool.Return sb
            }


        // --- STEP HEADERS ---

        member _.GetAllStepHeaders (): StepHeader list =
            connection
            |> Sql.existingConnection
            |> Sql.query "SELECT * FROM common.step_headers"
            |> Sql.execute parseStepHeaderRow

        member this.TryGetStepHeadersForRun (RunUid runUid' as runUid) =
            option {
                let! _ =
                    this.TryGetRunHeader runUid

                let allStepHeadersMap =
                    this.GetAllStepHeaders ()
                    |> Seq.map (fun hdr -> hdr.Uid.Value, hdr)
                    |> Map.ofSeq

                let stepUidsForRun =
                    connection
                    |> Sql.existingConnection
                    |> Sql.query $"SELECT step_uid FROM {schema}.run_steps WHERE run_uid = @run_uid"
                    |> Sql.parameters [ "run_uid", SqlValue.Uuid runUid' ]
                    |> Sql.execute (fun row -> row.uuid "step_uid")
                    |> Set

                let stepHeadersMapForRun =
                    allStepHeadersMap
                    |> Map.filter (fun stepUid _ -> stepUidsForRun.Contains stepUid)

                return
                    stepHeadersMapForRun 
            }


        // --- STEP RESULTS ---

        abstract member dtoToStepResults: 'TStepResultsDto -> Result<'TStepResults, string>

        abstract member stepResultsToDto: 'TStepResults -> Result<'TStepResultsDto, string>

        /// Constructs a mapping between the step Uids of the supplied run and
        /// functions that, when supplied a policy Id, will fetch the results for that
        /// step (provided they exist!). Note that requesting results for a non-existent run
        /// means that None will be returned. If successful, the map returned will indicate
        /// for which steps a getter is available.
        member this.CreateStepResultGetters (RunUid runUid' as runUid) =
            result {
                let! stepHeadersMapForRun =
                    this.TryGetStepHeadersForRun runUid
                    |> Result.requireSome (sprintf "Unable to locate step headers for run UID %A." runUid')

                let getter stepUid' policyId =
                    connection
                    |> Sql.existingConnection
                    |> Sql.query $"SELECT * FROM {schema}.step_results WHERE run_uid = @run_uid AND step_uid = @step_uid AND policy_id = @policy_id"
                    |> Sql.parameters [
                            "run_uid", SqlValue.Uuid runUid'
                            "step_uid", SqlValue.Uuid stepUid'
                            "policy_id", SqlValue.String policyId
                        ]
                    |> Sql.execute this.parseStepResultsRow
                    |> function
                        | []            -> Ok None
                        | [ Ok row ]    -> Ok (Some row)
                        | [ Error msg ] -> Error msg
                        | _ ->
                            failwithf "Multiple rows found for run UID %A, step UID %A and policy ID %s in table %s.step_results" runUid' stepUid' policyId schema

                let getters =
                    stepHeadersMapForRun
                    |> Map.map (fun stepUid _ -> getter stepUid)

                return getters                                 
            }        
       

        // --- POLICY DATA ---

        /// Returns a set of policy IDs for the given extraction UID. However, if no
        /// extraction header exists for the given UID, then None is returned.
        member this.TryGetPolicyIds (ExtractionUid extractionUid' as extractionUid) =
            option {
                // Check to see if we have a record of this extraction UID.
                let! _ =
                    this.TryGetExtractionHeader extractionUid

                let policyIds =
                    connection
                    |> Sql.existingConnection
                    |> Sql.query $"SELECT policy_id FROM {schema}.policy_data WHERE extraction_uid = @uid"
                    |> Sql.parameters [ "uid", SqlValue.Uuid extractionUid' ]
                    |> Sql.execute (fun row -> row.string "policy_id")
                    |> Set.ofList

                return policyIds
            }

        /// Returns a tuple of exited, remaining and new policy IDs. In each case, the IDs
        /// are returned as a set of strings.
        member this.GetPolicyIdDifferences (openingExtractionUid, closingExtractionUid) =
            result {
                let! openingPolicyIds =
                    this.TryGetPolicyIds openingExtractionUid
                    |> Result.requireSome (sprintf "Unable to locate opening extraction Uid %A." openingExtractionUid)
                    
                let! closingPolicyIds =
                    this.TryGetPolicyIds closingExtractionUid
                    |> Result.requireSome (sprintf "Unable to locate closing extraction Uid %A." closingExtractionUid)

                let exitedPolicyIds =
                    Set.difference openingPolicyIds closingPolicyIds

                let newPolicyIds =
                    Set.difference closingPolicyIds openingPolicyIds

                let remainingPolicyIds =
                    Set.intersect openingPolicyIds closingPolicyIds

                return exitedPolicyIds, remainingPolicyIds, newPolicyIds
            }
                  
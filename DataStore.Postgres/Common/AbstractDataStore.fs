 
namespace AnalysisOfChangeEngine.DataStore.Postgres


[<AutoOpen>]
module AbstractDataStore =

    open System
    open System.Text
    open FSharp.Quotations
    open Npgsql
    open FsToolkit.ErrorHandling
    open AnalysisOfChangeEngine
    open AnalysisOfChangeEngine.Controller
    open AnalysisOfChangeEngine.DataStore.Postgres.DataTransferObjects


    // Only required because nameof cannot work with non-static members.
    let private fieldName<'TRecord, 'TValue> (field: Expr<'TRecord -> 'TValue>) =
        match field with
        | Patterns.Lambda (_, Patterns.PropertyGet (_, propInfo, _)) ->
            propInfo.Name
        | _ ->
            failwith "Invalid field expression."


    [<NoEquality; NoComparison>]
    type IdentifyOutstandingRecordsOptions =
        {
            ReRunFailedCases : bool
        }
        

    [<AbstractClass>]
    type AbstractPostgresDataStore<'TPolicyRecord, 'TPolicyRecordDTO, 'TStepResults, 'TStepResultsDTO>
        (sessionContext: SessionContext, dataSource: NpgsqlDataSource, schema: string) =

        // --- DISPATCHERS ---

        let dataStageDispatcher =
            DataStageDTO.buildDispatcher<'TPolicyRecordDTO>
                (schema, dataSource)

        let extractionHeaderDispatcher =
            ExtractionHeaderDTO.buildDispatcher
                (schema, dataSource)

        let policyDataDispatcher =
            PolicyDataDTO.builderDispatcher<'TPolicyRecordDTO>
                (schema, dataSource)

        let runHeaderDispatcher =
            RunHeaderDTO.buildDispatcher
                (schema, dataSource)

        let runStepDispatcher =
            RunStepDTO.buildDispatcher
                (schema, dataSource)

        let stepHeaderDispatcher =
            StepHeaderDTO.buildDispatcher
                (dataSource)

        let stepResultsDispatcher =
            StepResultsDTO.buildDispatcher<'TStepResultsDTO>
                (schema, dataSource)

        let runFailureDispatcher =
            RunFailureDTO.buildDispatcher
                (schema, dataSource)


        // --- UID RESOLVER ---

        member this.CreateStepUidResolver () =
            let stepHeaders =
                this.GetAllStepHeaders ()
                // Should this be a map using StepUid's as keys?
                // Given that this is only going to be used by the backing machinery, it
                // seems overly cautious.
                |> Seq.map (fun sh -> sh.Uid.Value, sh)
                |> Map.ofSeq

            fun uid ->
                let stepHeader =
                    stepHeaders[uid]

                stepHeader.Title, stepHeader.Description


        // --- EXTRACTION HEADERS ---

        member _.TryGetExtractionHeader runUid =
            extractionHeaderDispatcher.TryGetByUid runUid
            |> Option.map DataTransferObjects.ExtractionHeaderDTO.toUnderlying


        // --- POLICY DATA ---

        abstract member dtoToPolicyRecord: 'TPolicyRecordDTO -> Result<'TPolicyRecord, string list>

        abstract member policyRecordToDTO: 'TPolicyRecord -> Result<'TPolicyRecordDTO, string list>


        // --- RUNS ---

        member _.TryGetRunHeader runUid =
            runHeaderDispatcher.TryGetByUid runUid
            |> Option.map DataTransferObjects.RunHeaderDTO.toUnderlying

        member this.CreateRun (title, comments, priorRunUid, closingRunDate, policyDataExtractionUid, walk: #IWalk) =
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
                    PriorRunUid             = priorRunUid
                    ClosingRunDate          = closingRunDate
                    PolicyDataExtractionUid = policyDataExtractionUid
                }

            let newRowDTO =
                DataTransferObjects.RunHeaderDTO.fromUnderlying newRow

            // TODO - Could this all be done as part of the same transaction?

            do runHeaderDispatcher.InsertRow newRowDTO

            for idx, step in List.indexed allWalkSteps
                do runStepDispatcher.InsertRow
                    { run_uid = newUid; step_idx = int16 idx; step_uid = step.Uid }

            RunUid newUid


        // --- STEP HEADERS ---

        member _.GetAllStepHeaders () : StepHeader list =
            stepHeaderDispatcher.SelectAll ()
            |> List.map DataTransferObjects.StepHeaderDTO.toUnderlying

        // This will return the step headers in the same order as the steps themselves
        // within the walk itself.
        member this.TryGetStepHeadersForRun runUid =
            option {
                let! _ =
                    this.TryGetRunHeader runUid

                let allStepHeadersMap =
                    this.GetAllStepHeaders ()
                    |> Seq.map (fun hdr -> hdr.Uid.Value, hdr)
                    |> Map.ofSeq

                let stepHeadersForRun =
                    runStepDispatcher.GetByRunUid runUid
                    |> List.sortBy _.step_idx
                    |> List.map (fun runStep -> allStepHeadersMap[runStep.step_uid])

                return
                    stepHeadersForRun
            }


        // --- STEP RESULTS ---

        abstract member dtoToStepResults: 'TStepResultsDTO -> Result<'TStepResults, string array>

        abstract member stepResultsToDTO: 'TStepResults -> Result<'TStepResultsDTO, string array>


        // --- POLICY DATA ---

        member this.GetPolicyRecordsAsync extractionUid policyIds =
            backgroundTask {
                let! records =
                    policyDataDispatcher.GetPolicyRecordsAsync extractionUid policyIds

                let records' =
                    records
                    |> Map.map (fun _ ->
                        this.dtoToPolicyRecord >> Result.mapError PolicyGetterFailure.ParseFailure)

                let records'' =
                    policyIds
                    |> Seq.filter (not << records'.ContainsKey)
                    |> Seq.fold (fun map pid ->
                        map |> Map.add pid (Error PolicyGetterFailure.NotFound)) records'

                return records''
            }
            

        member this.TryGetOutstandingRecords currentRunUid (options: IdentifyOutstandingRecordsOptions) =
            result {
                let! currentRunHeader =
                    this.TryGetRunHeader currentRunUid
                    |> Result.requireSome (sprintf "Unable to locate current run header '%O'" currentRunUid)

                let priorRunHeader =
                    match currentRunHeader.PriorRunUid with
                    // If a prior run ID is specified, it MUST exist!
                    | Some priorRunUid ->
                        match this.TryGetRunHeader priorRunUid with
                        | Some hdr ->
                            Some hdr
                        | None ->
                            failwithf "Unable to locate prior run header '%O'" priorRunUid
                    | _ ->
                        None


                (*
                Design Decision:
                    Why not implement this directly in the DB itself as a function or view?
                    However, any time we needed to change the logic, we'd need to update the
                    views in each of our Postgres schemas. Further, Postgres logic cannot
                    be parameterised in terms of the schema name.
                    As such, it's easier to dynamically generate the SQL logic here.
                *)
                // TODO - A tidier way to do this?
                // TODO - At least create a helper function that can extract the Postgres type-name
                // specified in the PG attribute associated with the type.
                // Regardless, we use aliases as a way to avoid the need to keep extracting field names.

                let sqlCommand =
                    new StringBuilder ()

                do ignore <| sqlCommand.Append(
                    $"""
                    WITH
                        run_steps AS (
                            SELECT
                                rs.{fieldName<RunStepDTO, _> <@ _.step_idx @>} AS idx,
                                sh.{fieldName<StepHeaderDTO, _> <@ _.uid @>} AS uid,
                                sh.{fieldName<StepHeaderDTO, _> <@ _.run_if_exited_record @>} AS run_if_exited_record,
                                sh.{fieldName<StepHeaderDTO, _> <@ _.run_if_new_record @>} AS run_if_new_record
                            FROM {schema}.{runStepDispatcher.PgTableName} AS rs
                            LEFT JOIN common.{stepHeaderDispatcher.PgTableName} AS sh
                            ON rs.{fieldName<RunStepDTO, _> <@ _.step_uid @>} =
                                sh.{fieldName<StepHeaderDTO, _> <@ _.uid @>}
                            WHERE rs.{fieldName<RunStepDTO, _> <@ _.run_uid @>} =
                                @current_run_uid
                        ),
    
                        opening_policy_ids AS (
                            SELECT DISTINCT {fieldName<PolicyData_BaseDTO, _> <@ _.policy_id @>} AS policy_id
                            FROM {schema}.{policyDataDispatcher.PgTableName}
                            WHERE {fieldName<PolicyData_BaseDTO, _> <@ _.extraction_uid @>} =
                                @prior_extraction_uid
                        ),
    
                        closing_policy_ids AS (
                            SELECT DISTINCT {fieldName<PolicyData_BaseDTO, _> <@ _.policy_id @>} AS policy_id
                            FROM {schema}.{policyDataDispatcher.PgTableName}
                            WHERE {fieldName<PolicyData_BaseDTO, _> <@ _.extraction_uid @>} =
                                @current_extraction_uid
                        ),
    
                        remaining_policy_ids AS (
                            SELECT policy_id
                            FROM opening_policy_ids
                            INTERSECT SELECT policy_id
                            FROM closing_policy_ids
                        ),
    
                        exited_policy_ids AS (
                            SELECT policy_id
                            FROM opening_policy_ids
                            EXCEPT SELECT policy_id
                            FROM closing_policy_ids
                        ),
    
                        new_policy_ids AS (
                            SELECT policy_id
                            FROM closing_policy_ids
                            EXCEPT SELECT policy_id
                            FROM opening_policy_ids
                        ),
    
                        steps_run AS (
                            SELECT
                                {fieldName<StepResults_BaseDTO, _> <@ _.policy_id @>} AS policy_id,
                                {fieldName<StepResults_BaseDTO, _> <@ _.step_uid @>} AS step_uid
                            FROM {schema}.{stepResultsDispatcher.PgTableName}
                            WHERE {fieldName<StepResults_BaseDTO, _> <@ _.run_uid @>} =
                                @current_run_uid
                        ),
    
                        steps_expected AS (
                            SELECT policy_id, uid AS step_uid
                            FROM exited_policy_ids, run_steps
                            WHERE run_if_exited_record
                            UNION ALL SELECT policy_id, uid AS step_uid
                            FROM remaining_policy_ids, run_steps
                            UNION ALL SELECT policy_id, uid AS step_uid
                            FROM new_policy_ids, run_steps
                            WHERE run_if_new_record
                        ),

                        failed_cases AS (
                            SELECT DISTINCT
                                {fieldName<RunFailureDTO, _> <@ _.policy_id @>} AS policy_id
                            FROM {schema}.{runFailureDispatcher.PgTableName}
                            WHERE {fieldName<RunFailureDTO, _> <@ _.run_uid @>} =
                                @current_run_uid
                        ),
    
                        step_statuses AS (
                            SELECT
                                COALESCE (expected.policy_id, run.policy_id) AS policy_id,
                                COALESCE (expected.step_uid, run.step_uid) AS step_uid,
                                run.policy_id IS NOT NULL AND expected.policy_id IS NOT NULL AS was_run
                            FROM steps_expected AS expected
                            LEFT JOIN steps_run AS run
                            ON expected.policy_id = run.policy_id
                                AND expected.step_uid = run.step_uid
                        ),

                        incomplete_step_results AS (
                            SELECT policy_id
                            FROM step_statuses
                            -- This approach seems MUCH faster than using a SELECT DISTINCT and a WHERE.
                            GROUP BY policy_id		
                            HAVING NOT bool_and(was_run)
                        ),
                    """
                )               
                
                if options.ReRunFailedCases then
                    do ignore <| sqlCommand.Append(
                        $"""
                        outstanding_cases AS (
                            SELECT policy_id
                            FROM incomplete_step_results
                        ),                    
                        """
                    )

                else
                    do ignore <| sqlCommand.Append(
                        $"""
                        outstanding_cases AS (
                            SELECT policy_id
                            FROM incomplete_step_results
                            WHERE policy_id NOT IN (SELECT policy_id FROM failed_cases)
                        ),
                        """
                    )

                do ignore <| sqlCommand.Append(
                    $"""
                        delete_existing_run_failures AS (
                            DELETE FROM {schema}.{runFailureDispatcher.PgTableName}
                            WHERE {fieldName<RunFailureDTO, _> <@ _.run_uid @>} = @current_run_uid
                                AND {fieldName<RunFailureDTO, _> <@ _.policy_id @>}
                                    IN (SELECT policy_id FROM outstanding_cases)
                        ),

                        -- ALthough unlikely, remove any existing step results for these records.
                        delete_existing_results AS (
                            DELETE FROM {schema}.{stepResultsDispatcher.PgTableName}
                            WHERE {fieldName<StepResults_BaseDTO, _> <@ _.run_uid @>} = @current_run_uid
                                AND {fieldName<StepResults_BaseDTO, _> <@ _.policy_id @>}
                                    IN (SELECT policy_id FROM outstanding_cases)
                        ),

                        delete_existing_data_stages AS (
                            DELETE FROM {schema}.{dataStageDispatcher.PgTableName}
                            WHERE {fieldName<DataStage_BaseDTO, _> <@ _.run_uid @>} = @current_run_uid
                                AND {fieldName<DataStage_BaseDTO, _> <@ _.policy_id @>}
                                    IN (SELECT policy_id FROM outstanding_cases)
                        )

                    SELECT
                        policy_id,
                        CASE
                            WHEN policy_id IN (SELECT policy_id FROM exited_policy_ids)
                                THEN 'EXITED'::common.cohort_membership
                            WHEN policy_id IN (SELECT policy_id FROM remaining_policy_ids)
                                THEN 'REMAINING'::common.cohort_membership
                            WHEN policy_id IN (SELECT policy_id FROM new_policy_ids)
                                THEN 'NEW'::common.cohort_membership
                        END AS cohort
                    FROM outstanding_cases;
                    """                    
                )

                let sqlCommand' =
                    sqlCommand.ToString ()

                use dbCommand = 
                    dataSource.CreateCommand sqlCommand'

                let currentRunUidParam =
                    new NpgsqlParameter<Guid>
                        ("@current_run_uid", currentRunUid.Value)

                let currentExtractionUidParam =
                    new NpgsqlParameter<Guid>
                        ("@current_extraction_uid", currentRunHeader.PolicyDataExtractionUid.Value)

                let priorExtractionUidParam: NpgsqlParameter =
                    match priorRunHeader with
                    | Some hdr ->
                        new NpgsqlParameter<Guid>
                            ("@prior_extraction_uid", hdr.PolicyDataExtractionUid.Value)
                    | None ->
                        // Cannot use NpgsqlParameter<Guid> here as DBNull.Value cannot be cast to a Guid.
                        new NpgsqlParameter
                            ("@prior_extraction_uid", DBNull.Value)

                do ignore <| dbCommand.Parameters.Add currentRunUidParam
                do ignore <| dbCommand.Parameters.Add currentExtractionUidParam
                do ignore <| dbCommand.Parameters.Add priorExtractionUidParam

                use dbReader =
                    dbCommand.ExecuteReader ()

                return [
                    while dbReader.Read () do
                        yield OutstandingPolicyIdDTO.recordParser dbReader
                ]
            }


        // --- INTERFACE FACTORIES ---

        member this.CreatePolicyGetter (extractionUid: ExtractionUid) =
            {
                new IPolicyGetter<'TPolicyRecord> with
                    member _.GetPolicyRecordsAsync policyIds =
                        this.GetPolicyRecordsAsync extractionUid policyIds
            }

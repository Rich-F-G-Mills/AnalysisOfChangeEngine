 
namespace AnalysisOfChangeEngine.DataStore.Postgres


[<AutoOpen>]
module AbstractDataStore =

    open System
    open Npgsql
    open FsToolkit.ErrorHandling
    open AnalysisOfChangeEngine
    open AnalysisOfChangeEngine.DataStore.Postgres.DataTransferObjects
        

    [<AbstractClass>]
    type AbstractDataStore<'TPolicyRecord, 'TPolicyRecordDTO, 'TStepResults, 'TStepResultsDTO>
        (sessionContext: SessionContext, dataSource: NpgsqlDataSource, schema: string) =

        // --- DISPATCHERS ---

        let extractionHeaderDispatcher =
            ExtractionHeaderDTO.buildDispatcher (schema, dataSource)

        let policyDataDispatcher =
            PolicyDataDTO.builderDispatcher<'TPolicyRecordDTO> (schema, dataSource)

        let runHeaderDispatcher =
            RunHeaderDTO.buildDispatcher (schema, dataSource)

        let runStepDispatcher =
            DataTransferObjects.RunStepDTO.buildDispatcher (schema, dataSource)

        let stepHeaderDispatcher =
            StepHeaderDTO.buildDispatcher (dataSource)

        let stepResultsDispatcher =
            StepResultsDTO.buildDispatcher<'TStepResultsDTO> (schema, dataSource)

        let stepValidationIssuesDispatcher =
            StepValidationIssuesDTO.buildDispatcher (schema, dataSource)



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

        member _.CreateExtractionHeader (extractionDate: DateOnly) =
            let newUid =
                Guid.NewGuid ()

            let newRow =
                {
                    Uid = ExtractionUid newUid
                    ExtractionDate = extractionDate
                }

            let newRowDTO =                
                DataTransferObjects.ExtractionHeaderDTO.fromUnderlying newRow

            extractionHeaderDispatcher.InsertRow newRowDTO

        member _.TryGetExtractionHeader runUid =
            extractionHeaderDispatcher.TryGetByUid runUid
            |> Option.map DataTransferObjects.ExtractionHeaderDTO.toUnderlying

        member _.GetAllExtractionHeaders () =
            extractionHeaderDispatcher.SelectAll ()
            |> List.map DataTransferObjects.ExtractionHeaderDTO.toUnderlying


        // --- POLICY DATA ---

        abstract member dtoToPolicyRecord: 'TPolicyRecordDTO -> Result<'TPolicyRecord, string>

        abstract member policyRecordToDTO: 'TPolicyRecord -> Result<'TPolicyRecordDTO, string>


        // --- RUNS ---

        member _.TryGetRunHeader runUid =
            runHeaderDispatcher.TryGetByUid runUid
            |> Option.map DataTransferObjects.RunHeaderDTO.toUnderlying

        member _.GetAllRunHeaders () =
            runHeaderDispatcher.SelectAll ()
            |> List.map DataTransferObjects.RunHeaderDTO.toUnderlying

        member this.CreateRun (title, comments, priorRunUid, closingRunDate, policyDataExtractionUid, walk: #AbstractWalk<_, _, _>) =
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

            newRow


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

        abstract member dtoToStepResults: 'TStepResultsDTO -> Result<'TStepResults, string>

        abstract member stepResultsToDTO: 'TStepResults -> Result<'TStepResultsDTO, string>

        /// Constructs a mapping between the step Uids of the supplied run and
        /// functions that, when supplied a policy Id, will fetch the results for that
        /// step (provided they exist!). Note that requesting results for a non-existent run
        /// means that None will be returned. If successful, the map returned will indicate
        /// for which steps a getter is available.
        member this.CreateStepResultGetters runUid =
            result {
                let! stepHeadersForRun =
                    this.TryGetStepHeadersForRun runUid
                    |> Result.requireSome "Unable to locate step headers for run."

                let getters =
                    stepHeadersForRun
                    |> Seq.map (fun hdr ->
                        hdr.Uid, stepResultsDispatcher.TryGetRows runUid hdr.Uid)
                    |> Map.ofSeq

                return getters
            }
            
        member _.DeleteResultsForPolicy stepUid policyId =
            0


        // --- POLICY DATA ---

        /// Returns a tuple of exited, remaining and new policy IDs. In each case, the IDs
        /// are returned as a set of strings.
        member this.TryGetOutstandingRecords currentRunUid =
            result {
                let! currentRunHeader =
                    this.TryGetRunHeader currentRunUid
                    |> Result.requireSome (sprintf "Unable to locate current run header '%O'" currentRunUid)

                // We don't get about the closing extraction header, only that it exists!
                let! _ =
                    this.TryGetExtractionHeader currentRunHeader.PolicyDataExtractionUid
                    |> Result.requireSome
                        (sprintf "Unable to locate current extraction header '%O'" currentRunHeader.PolicyDataExtractionUid)

                let! priorRunHeader =
                    match currentRunHeader.PriorRunUid with
                    | Some priorRunUid ->
                        match this.TryGetRunHeader priorRunUid with
                        | Some hdr ->
                            Ok (Some hdr)
                        | None ->
                            Error (sprintf "Unable to locate prior run header '%O'" priorRunUid)
                    | _ ->
                        Ok None

                // As above, we only care that the prior extraction header exists,
                // provided a prior run is present.
                let! _ =
                    match priorRunHeader with
                    | Some runHdr ->
                        match this.TryGetExtractionHeader runHdr.PolicyDataExtractionUid with
                        | Some extractionHdr ->
                            Ok (Some extractionHdr)
                        | None ->
                            Error (sprintf "Unable to locate opening extraction header '%O'" runHdr.PolicyDataExtractionUid)
                    | None ->
                        Ok None

                let sqlCommand =
                    $"""
                    WITH
                        run_steps AS (
		                    SELECT rs.step_idx AS idx, sh.*
		                    FROM {schema}.run_steps AS rs
		                    LEFT JOIN common.step_headers AS sh
		                    ON rs.step_uid = sh.uid
		                    WHERE run_uid = @current_run_uid
	                    ),
	
	                    opening_policy_ids AS (
		                    SELECT DISTINCT policy_id
		                    FROM {schema}.policy_data
		                    WHERE extraction_uid = @prior_extraction_uid
	                    ),
	
	                    closing_policy_ids AS (
		                    SELECT DISTINCT policy_id
		                    FROM {schema}.policy_data
		                    WHERE extraction_uid = @current_extraction_uid
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
	
	                    run_failures AS (
		                    SELECT *
		                    FROM {schema}.run_failures
		                    WHERE run_uid = @current_run_uid
	                    ),
	
	                    steps_run AS (
		                    SELECT policy_id, step_uid
		                    FROM {schema}.step_results
		                    WHERE run_uid = @current_run_uid
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
	
	                    step_statuses AS (
		                    SELECT
			                    COALESCE (expected.policy_id, run.policy_id) AS policy_id,
			                    COALESCE (expected.step_uid, run.step_uid) AS step_uid,
			                    run.policy_id IS NOT NULL AND expected.policy_id IS NOT NULL AS was_run
		                    FROM steps_expected AS expected
		                    LEFT JOIN steps_run AS run
		                    ON expected.policy_id = run.policy_id
			                    AND expected.step_uid = run.step_uid		
	                    )

		            SELECT
			            policy_id,
			            policy_id IN (SELECT policy_id FROM run_failures) AS had_run_failure,
                        CASE
		                    WHEN policy_id IN (SELECT policy_id FROM exited_policy_ids)
			                    THEN 'EXITED'::common.cohort_membership
		                    WHEN policy_id IN (SELECT policy_id FROM remaining_policy_ids)
			                    THEN 'REMAINING'::common.cohort_membership
		                    WHEN policy_id IN (SELECT policy_id FROM new_policy_ids)
			                    THEN 'NEW'::common.cohort_membership
	                    END AS cohort
		            FROM step_statuses
		            -- This approach seems MUCH faster than using a SELECT DISTINCT and a WHERE.
		            GROUP BY policy_id		
		            HAVING NOT bool_and(was_run)
                    """

                use dbCommand = 
                    dataSource.CreateCommand (sqlCommand)

                let currentRunUidParam =
                    new NpgsqlParameter<Guid>
                        ("@current_run_uid", currentRunUid.Value)

                let currentExtractionUidParam =
                    new NpgsqlParameter<Guid>
                        ("@current_extraction_uid", currentRunHeader.PolicyDataExtractionUid.Value)

                let priorExtractionUidParam =
                    match priorRunHeader with
                    | Some hdr ->
                        new NpgsqlParameter<Guid>
                            ("@prior_extraction_uid", hdr.PolicyDataExtractionUid.Value)
                    | None ->
                        new NpgsqlParameter<Guid> (Value = DBNull.Value)

                do ignore <| dbCommand.Parameters.Add currentRunUidParam
                do ignore <| dbCommand.Parameters.Add currentExtractionUidParam
                do ignore <| dbCommand.Parameters.Add priorExtractionUidParam

                use dbReader =
                    dbCommand.ExecuteReader ()

                return [
                    while dbReader.Read () do
                        yield OutstandingRecordDTO.recordParser dbReader
                ]
            }


 
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

        let openingDataStageUid =
            Guid("00000000-0000-0000-0000-000000000000")

        let closingDataStageUid =
            Guid("FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF")


        // --- UID RESOLVER ---

        member this.CreateStepUidResolver () =
            let stepHeaders =
                this.GetAllStepHeaders ()
                // Should this be a map using StepUid's as keys?
                // Given that this is only going to be used by the backing machinery, it
                // seems overly cautious.
                |> Seq.map (fun sh -> sh.StepUid.Value, sh)
                |> Map.ofSeq

            fun uid ->
                let stepHeader =
                    stepHeaders[uid]

                stepHeader.Title, stepHeader.Description


        // --- EXTRACTION HEADERS ---

        member _.TryGetExtractionHeader runUid =
            extractionHeaderDispatcher.TryGetByUid runUid
            |> Option.map DataTransferObjects.ExtractionHeaderDTO.toUnderlying


        // --- RUNS ---

        member _.TryGetRunHeader runUid =
            runHeaderDispatcher.TryGetByUid runUid
            |> Option.map DataTransferObjects.RunHeaderDTO.toUnderlying

        member this.CreateRun (title, comments, priorRunUid, closingRunDate, policyDataExtractionUid, walk: #IWalk) =
            let stepHeaders =
                this.GetAllStepHeaders ()
                |> Seq.map (fun hdr -> hdr.StepUid.Value)
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
                    RunUid                  = RunUid newUid
                    Title                   = title
                    Comments                = comments
                    CreatedBy               = sessionContext.UserName
                    CreatedWhen             = DateTime.Now
                    PriorRunUid             = priorRunUid
                    ClosingRunDate          = closingRunDate
                    PolicyDataExtractionUid = policyDataExtractionUid
                    ClosingStepUid          = StepUid walk.ClosingStep.Uid
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
                    |> Seq.map (fun hdr -> hdr.StepUid.Value, hdr)
                    |> Map.ofSeq

                let stepHeadersForRun =
                    runStepDispatcher.GetByRunUid runUid
                    |> List.sortBy _.step_idx
                    |> List.map (fun runStep -> allStepHeadersMap[runStep.step_uid])

                return
                    stepHeadersForRun
            }


        // --- STEP RESULTS ---

        abstract member dtoToStepResults: 'TStepResultsDTO -> Result<'TStepResults, string list>

        abstract member stepResultsToDTO: 'TStepResults -> Result<'TStepResultsDTO, string list>

        member this.GetStepResultsAsync runUid stepUid policyIds =
            backgroundTask {
                let! records =
                    stepResultsDispatcher.GetRowsAsync runUid stepUid policyIds

                let records' =
                    records
                    |> Map.map (fun _ -> this.dtoToStepResults)

                return records'
            }


        // --- POLICY DATA ---

        abstract member dtoToPolicyRecord: 'TPolicyRecordDTO -> Result<'TPolicyRecord, string list>

        abstract member policyRecordToDTO: 'TPolicyRecord -> Result<'TPolicyRecordDTO, string list>

        member this.GetPolicyRecordsAsync extractionUid policyIds =
            backgroundTask {
                let! records =
                    policyDataDispatcher.GetPolicyRecordsAsync extractionUid policyIds

                let records' =
                    records
                    |> Map.map (fun _ -> this.dtoToPolicyRecord)

                return records'
            }
            

        member this.TryGetOutstandingRecords currentRunUid (options: IdentifyOutstandingRecordsOptions) =
            result {
                let! currentRunHeader =
                    this.TryGetRunHeader currentRunUid
                    |> Result.requireSome (sprintf "Unable to locate current run header '%O'" currentRunUid)

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
                    WITH outstanding_cases AS (
                        SELECT policy_id, cohort
                        FROM {schema}.policy_tracing
                        WHERE run_uid = @current_run_uid AND NOT all_steps_run                   
                    """
                    )        
                
                if not options.ReRunFailedCases then
                    do ignore <| sqlCommand.Append(" AND NOT had_failure")

                do ignore <| sqlCommand.Append(
                    $"""
                        ), delete_existing_run_failures AS (
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
                        policy_id AS {fieldName<OutstandingPolicyIdDTO, _> <@ _.policy_id @>},
                        cohort AS {fieldName<OutstandingPolicyIdDTO, _> <@ _.cohort @>}
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

                do ignore <| dbCommand.Parameters.Add currentRunUidParam

                use dbReader =
                    dbCommand.ExecuteReader ()

                return [
                    while dbReader.Read () do
                        yield OutstandingPolicyIdDTO.recordParser dbReader
                ]
            }


        // --- INTERFACE FACTORIES ---

        member this.CreatePolicyGetter extractionUid =
            {
                new IPolicyGetter<_> with
                    member _.GetPolicyRecordsAsync policyIds =
                        this.GetPolicyRecordsAsync extractionUid policyIds
            }

        member this.CreateStepResultsGetter runUid stepUid =
            {
                new IStepResultsGetter<_> with
                    member _.GetStepResultsAsync policyIds =
                        this.GetStepResultsAsync runUid stepUid policyIds
            }


        member private this.ConstructDataChangeWriteCommands
            (constructBaseDataStageDTO, constructDataStageWriteFailureDTO) =
                Map.toSeq
                // Use the applicative variant so that we get all errors, rather than
                // just the first.
                >> Seq.traverseResultA (fun (dataStageUid, policyRecord) ->
                    result {                                                    
                        let constructDataChangeWriteFailureCommands = function
                            // If no reasons provided, then make sure None is passed to the DTO.
                            | [] ->
                                failwith "No reasons provided for data stage write failure."
                            | reasons ->
                                reasons
                                |> Seq.map (constructDataStageWriteFailureDTO dataStageUid)                            
                                |> Seq.map runFailureDispatcher.GetRowInsertBatchCommand

                        let! policyRecordDTO =
                            this.policyRecordToDTO policyRecord
                            |> Result.mapError constructDataChangeWriteFailureCommands

                        let dataStageBaseDTO =
                            constructBaseDataStageDTO dataStageUid
                                                        
                        return dataStageDispatcher.GetRowInsertBatchCommand
                            (dataStageBaseDTO, policyRecordDTO)
                    })
                // Convert everything to a sequence.
                >> Result.eitherMap Seq.ofArray Seq.concat

        member private this.ConstructStepResultsWriteCommands
            (constructBaseStepResultDTO, constructRunFailureDTO) =
                Map.toSeq
                >> Seq.traverseResultA (fun (stepUid, (stepDataSource, stepResults)) ->
                    result {
                        let dataStageUsedDTO =
                            match stepDataSource with
                            | StepDataSource.OpeningData ->
                                openingDataStageUid
                            | StepDataSource.DataChangeStep dataChangeStepUid ->
                                dataChangeStepUid.Uid
                            | StepDataSource.ClosingData ->
                                closingDataStageUid

                        let constructStepResultWriteFailureCommands = function
                            // As above, make sure we pass in None if no reasons were provided.
                            | [] ->
                                failwith "No reasons provided for step write failure."
                            | reasons ->
                                reasons
                                |> Seq.map (constructRunFailureDTO stepUid)
                                |> Seq.map runFailureDispatcher.GetRowInsertBatchCommand

                        let stepResultsBaseDTO =
                            constructBaseStepResultDTO (stepUid, dataStageUsedDTO)

                        let! stepResultsDTO =
                            this.stepResultsToDTO stepResults
                            |> Result.mapError constructStepResultWriteFailureCommands
                                                        
                        return stepResultsDispatcher.GetRowInsertBatchCommand
                            (stepResultsBaseDTO, stepResultsDTO)
                    })
                // Convert everything to a sequence.
                >> Result.eitherMap Seq.ofArray Seq.concat

        static member private ProcessSuccessfulWalk
            (constructDataChangeWriteCommands, constructStepResultsWriteCommands)
            (outcome: EvaluatedPolicyWalk<'TPolicyRecord, 'TStepResults>) =
                result {                                         
                    // First, let's see if we can construct all of
                    // the required data change write commands.
                    let! dataChangeWrites =
                        constructDataChangeWriteCommands outcome.InteriorDataChanges                               

                    // ...before trying to construct the step result
                    // write commands.
                    let! stepResultWrites =
                        constructStepResultsWriteCommands outcome.StepResults                        

                    // Provided we get this far, we can combine the
                    // write commands from above.
                    let combinedWriteCommands =
                        stepResultWrites
                        |> Seq.append dataChangeWrites

                    return combinedWriteCommands
                }

        // Before anyone says anything, I realise there are a lot of allocations
        // going on here. However, this function does NOT lie on the critical
        // path and I therefore do not anticipate this leading to undue GC pressure.
        member this.CreateOutputWriter (RunUid currentRunUid', SessionUid sessionUid') =
            let constructBaseStepResultDTO policyId (stepUid, usedDataStageUid) =
                {
                    run_uid             = currentRunUid'
                    session_uid         = sessionUid'
                    policy_id           = policyId
                    step_uid            = stepUid
                    used_data_stage_uid = usedDataStageUid
                }

            let constructBaseDataStageDTO policyId dataStageUid =
                {
                    run_uid             = currentRunUid'
                    session_uid         = sessionUid'
                    data_stage_uid      = dataStageUid
                    policy_id           = policyId                
                }

            let constructFailureDTO failureType policyId stepUid reason =
                {
                    run_uid             = currentRunUid'
                    session_uid         = sessionUid'
                    step_uid            = Some stepUid
                    policy_id           = policyId
                    failure_type        = failureType
                    reason              = Some reason                    
                }

            let constructDataStageWriteFailureDTO =
                constructFailureDTO RunFailureTypeDTO.DATA_CHANGE_WRITE_FAILURE               

            let constructStepResultsWriteFailureDTO =
                constructFailureDTO RunFailureTypeDTO.STEP_RESULTS_WRITE_FAILURE

            let constructProcessingFailureDTO =
                RunFailureDTO.constructFrom (currentRunUid', sessionUid')            

            {
                new IProcessedOutputWriter<'TPolicyRecord, 'TStepResults> with
                    member _.WriteProcessedOutputAsync processedOutputs =                       
                        backgroundTask {
                            let batchCommands =
                                processedOutputs
                                |> Array.map (fun processedOutput ->
                                    let constructBaseDataStageDTO' =
                                        constructBaseDataStageDTO processedOutput.PolicyId

                                    let constructBaseStepResultDTO' =
                                        constructBaseStepResultDTO processedOutput.PolicyId

                                    let constructDataStageWriteFailureDTO' =
                                        constructDataStageWriteFailureDTO processedOutput.PolicyId

                                    let constructStepResultsWriteFailureDTO' =
                                        constructStepResultsWriteFailureDTO processedOutput.PolicyId

                                    let constructProcessingFailureDTO' =
                                        constructProcessingFailureDTO processedOutput.PolicyId

                                    match processedOutput.WalkOutcome with
                                    | Ok outcome ->
                                        let constructDataChangeWriteCommands' =
                                            this.ConstructDataChangeWriteCommands
                                                (constructBaseDataStageDTO', constructDataStageWriteFailureDTO')

                                        let constructStepResultsWriteCommands' =
                                            this.ConstructStepResultsWriteCommands
                                                (constructBaseStepResultDTO', constructStepResultsWriteFailureDTO')

                                        let processSuccessfulWalk' =
                                            AbstractPostgresDataStore<_, _, _, _>.ProcessSuccessfulWalk
                                                (constructDataChangeWriteCommands', constructStepResultsWriteCommands')

                                        processSuccessfulWalk' outcome
                                        |> Result.either
                                            // Indicate that no failures have arisen from the write process.
                                            (fun cmds -> false, cmds)
                                            // ...or, that some have arisen.
                                            (fun cmds -> true, cmds)

                                    | Error processingFailure ->
                                        let insertionCommands =
                                            constructProcessingFailureDTO' processingFailure
                                            |> Seq.map runFailureDispatcher.GetRowInsertBatchCommand
                                            
                                        false, insertionCommands)

                            use batch =
                                dataSource.CreateBatch ()

                            do  batchCommands
                                |> Seq.collect snd
                                |> Seq.iter batch.BatchCommands.Add

                            // TODO - Safe to ignore the returned int?
                            let! _ =
                                batch.ExecuteNonQueryAsync ()

                            return
                                batchCommands
                                |> Array.map (fun (failuresGenerated, _) ->
                                    { FailuresGenerated = failuresGenerated })
                        }
            }

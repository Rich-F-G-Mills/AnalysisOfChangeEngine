 
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


        // --- STEP VALIDATION ISSUES ---

        member this.AddValidationIssues (RunUid runUid') (StepUid stepUid') policyId issues =
            let newRows =
                issues
                |> List.map (fun (classification, message) ->
                    {
                        run_uid         = runUid'
                        step_uid        = stepUid'
                        policy_id       = policyId
                        classification  = classification
                        message         = message
                    })    
                    
            do stepValidationIssuesDispatcher.InsertRows newRows
       

        // --- POLICY DATA ---

        /// Returns a set of policy IDs for the given extraction UID. However, if no
        /// extraction header exists for the given UID, then None is returned.
        member this.TryGetPolicyIds extractionUid =
            option {
                // Check to see if we have a record of this extraction UID.
                let! _ =
                    this.TryGetExtractionHeader extractionUid

                return policyDataDispatcher.GetPolicyIds extractionUid
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


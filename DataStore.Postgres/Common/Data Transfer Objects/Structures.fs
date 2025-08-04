
namespace AnalysisOfChangeEngine.DataStore.Postgres.DataTransferObjects

open System
open System.Data.Common
open System.Reflection
open System.Threading.Tasks
open FSharp.Reflection
open Npgsql
open AnalysisOfChangeEngine
open AnalysisOfChangeEngine.DataStore.Postgres


// EVERYTHING here should be internal! Nothing here should be escaping beyond
// the bounds of this assembly.


[<NoEquality; NoComparison>]
type internal DataStage_BaseDTO =
    {
        run_uid         : Guid
        data_stage_uid  : Guid
        policy_id       : string
    }

[<RequireQualifiedAccess>]
module internal DataStageDTO =

    let [<Literal>] private pgTableName =
        "data_stages"

    type internal IDispatcher =
        interface
            abstract member PgTableName     : string with get
        end

    let internal buildDispatcher<'TAugRowDTO> (schema, dataSource) =

        let dispatcher =
            new PostgresTableDispatcher<DataStage_BaseDTO, 'TAugRowDTO>
                (pgTableName, schema, dataSource)

        {
            new IDispatcher with
                member _.PgTableName
                    with get () = pgTableName                  
        }


[<NoEquality; NoComparison>]
type internal ExtractionHeaderDTO =
    {
        uid             : Guid
        extraction_date : DateOnly
    }

[<RequireQualifiedAccess>]
module internal ExtractionHeaderDTO =

    let [<Literal>] private pgTableName =
        "extraction_headers"

    type internal IDispatcher =
        interface
            abstract member SelectAll       : unit -> ExtractionHeaderDTO list
            abstract member TryGetByUid     : ExtractionUid -> ExtractionHeaderDTO option
            abstract member InsertRow       : ExtractionHeaderDTO -> unit
            abstract member PgTableName     : string with get
        end
        
    let internal buildDispatcher (schema, dataSource) =

        let dispatcher =
            new PostgresTableDispatcher<ExtractionHeaderDTO, Unit>
                (pgTableName, schema, dataSource)

        let selectByUid =
            dispatcher.MakeBaseEquality1Selector
                <@ _.uid @>

        let rowInserter =
            dispatcher.MakeRowInserter ()

        {
            new IDispatcher with
                member _.SelectAll () =
                    dispatcher.SelectAllBaseRecords ()

                member _.TryGetByUid (ExtractionUid extractionUid') =
                    match selectByUid.Execute extractionUid' with
                    | []    -> None
                    | [x]   -> Some x
                    | _     -> failwith "Multiple extraction headers found."

                member _.InsertRow row =
                    do ignore <| rowInserter.ExecuteNonQuery (row, ())

                member _.PgTableName
                    with get () = pgTableName                  
        }

    let internal toUnderlying (hdr: ExtractionHeaderDTO): ExtractionHeader =
        {
            Uid             = ExtractionUid hdr.uid
            ExtractionDate  = hdr.extraction_date            
        }

    let internal fromUnderlying (hdr: ExtractionHeader): ExtractionHeaderDTO =
        {
            uid             = hdr.Uid.Value
            extraction_date = hdr.ExtractionDate
        }


[<NoEquality; NoComparison>]
type internal PolicyData_BaseDTO =
    {
        extraction_uid          : Guid
        policy_id               : string        
    }

[<RequireQualifiedAccess>]
module internal PolicyDataDTO =

    let [<Literal>] private pgTableName =
        "policy_data"

    type internal IDispatcher<'TAugRowDTO> =
        interface
            abstract member GetPolicyRecordsAsync   : ExtractionUid -> string array -> Task<Map<string, 'TAugRowDTO>>
            abstract member PgTableName             : string with get
        end

    let internal builderDispatcher<'TAugRowDTO> (schema, dataSource) =
        let dispatcher =
            new PostgresTableDispatcher<PolicyData_BaseDTO, 'TAugRowDTO>
                (pgTableName, schema, dataSource)

        let recordGetter =
            dispatcher.MakeCombinedEquality1Multiple1Selector
                (<@ _.extraction_uid @>, <@ _.policy_id @>)
                
        {
            new IDispatcher<'TAugRowDTO> with

                member _.GetPolicyRecordsAsync (ExtractionUid extractionUid') policyIds =
                    backgroundTask {
                        let! records =
                            recordGetter.ExecuteAsync extractionUid' policyIds

                        // We're not gauranteed to get a response for all policy records.
                        // We return a map to make it explicit as to which records we have.
                        let records' =
                            records
                            |> Seq.map (fun (baseRec, augRec) -> baseRec.policy_id, augRec)
                            |> Map.ofSeq
                            
                        return records'
                    }

                member _.PgTableName
                    with get () = pgTableName                  
        }
            


[<NoEquality; NoComparison>]
type internal RunHeaderDTO =
    {
        uid                         : Guid
        title                       : string
        comments                    : string option
        created_by                  : string
        created_when                : DateTime
        prior_run_uid               : Guid option
        closing_run_date            : DateOnly
        policy_data_extraction_uid  : Guid        
    }

[<RequireQualifiedAccess>]
module internal RunHeaderDTO =

    let [<Literal>] private pgTableName =
        "run_headers"

    type internal IDispatcher =
        interface
            abstract member SelectAll       : unit -> RunHeaderDTO list
            abstract member TryGetByUid     : RunUid -> RunHeaderDTO option
            abstract member InsertRow       : RunHeaderDTO -> unit
            abstract member PgTableName     : string with get
        end
        
    let internal buildDispatcher (schema, dataSource) =
        let dispatcher =
            new PostgresTableDispatcher<RunHeaderDTO, Unit>
                (pgTableName, schema, dataSource)

        let selectByUid =
            dispatcher.MakeBaseEquality1Selector    
                <@ _.uid @>

        let rowInserter =
            dispatcher.MakeRowInserter ()

        {
            new IDispatcher with
                member _.SelectAll () =
                    dispatcher.SelectAllBaseRecords ()

                member _.TryGetByUid (RunUid runUid') =
                    match selectByUid.Execute runUid' with
                    | []    -> None
                    | [x]   -> Some x
                    | _     -> failwith "Multiple run headers found."

                member _.InsertRow row =
                    do ignore <| rowInserter.ExecuteNonQuery (row, ())

                member _.PgTableName
                    with get () = pgTableName                  
        }

    let internal toUnderlying (hdr: RunHeaderDTO): RunHeader =
        {
            Uid                         = RunUid hdr.uid
            Title                       = hdr.title
            Comments                    = hdr.comments
            CreatedBy                   = hdr.created_by
            CreatedWhen                 = hdr.created_when
            PriorRunUid                 = hdr.prior_run_uid |> Option.map RunUid 
            ClosingRunDate              = hdr.closing_run_date
            PolicyDataExtractionUid     = ExtractionUid hdr.policy_data_extraction_uid       
        }

    let internal fromUnderlying (hdr: RunHeader): RunHeaderDTO =
        {
            uid                         = hdr.Uid.Value
            title                       = hdr.Title
            comments                    = hdr.Comments
            created_by                  = hdr.CreatedBy
            created_when                = hdr.CreatedWhen
            prior_run_uid               = hdr.PriorRunUid |> Option.map _.Value
            closing_run_date            = hdr.ClosingRunDate
            policy_data_extraction_uid  = hdr.PolicyDataExtractionUid.Value
        }


[<NoEquality; NoComparison>]
type internal RunStepDTO =
    {
        run_uid                 : Guid
        step_idx                : Int16
        step_uid                : Guid
    }

[<RequireQualifiedAccess>]
module internal RunStepDTO =

    let [<Literal>] private pgTableName =
        "run_steps"

    type internal IDispatcher =
        interface
            abstract member GetByRunUid     : RunUid -> RunStepDTO list
            abstract member InsertRow       : RunStepDTO -> unit
            abstract member PgTableName     : string with get
        end

    let internal buildDispatcher (schema, dataSource) =
        let dispatcher =
            new PostgresTableDispatcher<RunStepDTO, Unit>
                (pgTableName, schema, dataSource)

        let selectByRunUid =
            dispatcher.MakeBaseEquality1Selector
                <@ _.run_uid @>

        let rowInserter =
            dispatcher.MakeRowInserter ()

        {
            new IDispatcher with
                member _.GetByRunUid (RunUid runUid') =
                    selectByRunUid.Execute runUid'

                member _.InsertRow row =
                    ignore <| rowInserter.ExecuteNonQuery (row, ())

                member _.PgTableName
                    with get () = pgTableName              
        }


[<RequireQualifiedAccess>]
[<NoEquality; NoComparison>]
[<PostgresCommonEnumeration("failure_type")>]
type internal RunFailureTypeDTO =
    | OPENING_RECORD_NOT_FOUND
    | CLOSING_RECORD_NOT_FOUND
    | OPENING_RECORD_PARSE_FAILURE
    | CLOSING_RECORD_PARSE_FAILURE
    | DATA_CHANGE_RECORD_FAILURE
    | VALIDATION_ABORTED
    | VALIDATION_FAILURE
    | API_CALL_FAILURE
    | API_CALCULATION_FAILURE
    | STEP_CONSTRUCTION_FAILURE    

[<NoEquality; NoComparison>]
type internal RunFailureDTO =
    {           
        run_uid                 : Guid
        policy_id               : string
        step_uid                : Guid option
        error_type              : RunFailureTypeDTO
        reason                  : string option   
    }

[<AbstractClass>]
module internal RunFailureDTO =
    
    let [<Literal>] private pgTableName =
        "run_failures"

    type internal IDispatcher =
        interface
            abstract member PgTableName     : string with get
        end

    let internal buildDispatcher (schema, dataSource) =
        let dispatcher =
            new PostgresTableDispatcher<RunFailureDTO, Unit>
                (pgTableName, schema, dataSource)

        {
            new IDispatcher with
                member _.PgTableName
                    with get () = pgTableName                  
        }

    
[<NoEquality; NoComparison>]
type internal StepHeaderDTO =
    {
        uid                     : Guid
        title                   : string
        description             : string
        run_if_exited_record    : bool
        run_if_new_record       : bool
    }
    
[<RequireQualifiedAccess>]
module internal StepHeaderDTO =

    let [<Literal>] private pgTableName =
        "step_headers"

    (*
    Design Decision:
        Rather than allow direct access to the underlying PostgresTable object,
        we define an interface which allows us to restrict the available behaviours.
        It also allows us to pre-compute certain functions such as filtered selectors.
    *)
    type internal IDispatcher =
        interface
            abstract member SelectAll       : unit -> StepHeaderDTO list
            abstract member TryGetByUid     : StepUid -> StepHeaderDTO option
            abstract member PgTableName     : string with get
        end

    let internal buildDispatcher dataSource =
        let dispatcher =
            new PostgresCommonTableDispatcher<StepHeaderDTO, Unit>
                (pgTableName, dataSource)

        let selectByUid =
            // Could we have used lazy instantiation here?
            // Although it would prevent code being generated unnecessarily,
            // the trade-off is that we could find issues occurring mid-flight.
            // It would be "best" if any issues occur as an exception
            // during assembly start-up code (ie... fail-safe AND fail-fast).
            dispatcher.MakeBaseEquality1Selector    
                <@ _.uid @>

        {
            new IDispatcher with
                member _.SelectAll () =
                    dispatcher.SelectAllBaseRecords ()
                        
                member _.TryGetByUid (StepUid stepUid') =
                    match selectByUid.Execute stepUid' with
                    | []    -> None
                    | [x]   -> Some x
                    | _     -> failwith "Multiple step headers found."

                member _.PgTableName
                    with get () = pgTableName
        }

    let internal toUnderlying (hdr: StepHeaderDTO): StepHeader =
        {
            Uid                 = StepUid hdr.uid
            Title               = hdr.title
            Description         = hdr.description
            RunIfExitedRecord   = hdr.run_if_exited_record
            RunIfNewRecord      = hdr.run_if_new_record
        }

    let internal fromUnderlying (hdr: StepHeader): StepHeaderDTO =
        {
            uid                     = hdr.Uid.Value
            title                   = hdr.Title
            description             = hdr.Description
            run_if_exited_record    = hdr.RunIfExitedRecord
            run_if_new_record       = hdr.RunIfNewRecord
        }


[<NoEquality; NoComparison>]
type internal StepResults_BaseDTO =
    {
        run_uid                 : Guid
        step_uid                : Guid
        // In theory, we could live without this. However, it does make it easier to
        // track down the policy characteristics run for a given step.
        used_data_stage_uid     : Guid option
        policy_id               : string
    }

[<RequireQualifiedAccess>]
module internal StepResultsDTO =

    let [<Literal>] private pgTableName =
        "step_results"
        
    type internal IDispatcher<'TAugRowDTO> =
        interface
            abstract member PgTableName                 : string with get
            abstract member GetRowsAsync                : RunUid -> StepUid -> string array -> Task<Map<string, 'TAugRowDTO>>
            abstract member GetRowInsertBatchCommand    : StepResults_BaseDTO * 'TAugRowDTO -> NpgsqlBatchCommand
        end

    let internal buildDispatcher<'TAugRowDTO> (schema, dataSource) =

        let dispatcher =
            PostgresTableDispatcher<StepResults_BaseDTO, 'TAugRowDTO>
                (pgTableName, schema, dataSource)

        let stepResultsSelector =
            dispatcher.MakeCombinedEquality2Multiple1Selector 
                (<@ _.run_uid @>, <@ _.step_uid @>, <@ _.policy_id @>)

        let stepResultsInserter =
            dispatcher.MakeRowInserter ()

        {
            new IDispatcher<'TAugRowDTO> with                  
                member _.PgTableName
                    with get () = pgTableName        
                    
                member _.GetRowsAsync (RunUid runUid') (StepUid stepUid') policyIds =
                    backgroundTask {
                        let! records =
                            stepResultsSelector.ExecuteAsync runUid' stepUid' policyIds

                        // We're not gauranteed to get a response for all policy records.
                        // We return a map to make it explicit as to which records we have.
                        let records' =
                            records
                            |> Seq.map (fun (baseRec, augRec) -> baseRec.policy_id, augRec)
                            |> Map.ofSeq
                            
                        return records'
                    }
                    
                member _.GetRowInsertBatchCommand (baseRow, augRow) = 
                    stepResultsInserter.AsBatchCommand (baseRow, augRow)
        }


[<RequireQualifiedAccess>]
[<NoEquality; NoComparison>]
[<PostgresCommonEnumeration("cohort_membership")>]
type private CohortMembershipDTO =
    | EXITED
    | REMAINING
    | NEW

[<NoEquality; NoComparison>]
type private OutstandingPolicyIdDTO =
    {
        policy_id               : string
        cohort                  : CohortMembershipDTO
    }

[<RequireQualifiedAccess>]
module internal OutstandingPolicyIdDTO =

    // Because we're not wrapping this DTO in a dispatcher, we have to create the record
    // parser ourselves.
    let private recordFields =
        FSharpType.GetRecordFields
            // We know the type is private, so we can be specific about our binding flags.
            (typeof<OutstandingPolicyIdDTO>, BindingFlags.NonPublic)

    let private recordTransferableTypes =
        recordFields
        |> Array.map (fun pi -> TransferableType.InvokeGetFor pi.PropertyType)

    // Type inferencing can't seem to cope without the type hint.
    let private dtoRecordParser : DbDataReader -> OutstandingPolicyIdDTO =
        RecordParser.Create<OutstandingPolicyIdDTO>
            (recordTransferableTypes, [| 0 .. recordFields.Length - 1 |])

    let private toUnderlying = function
        | { policy_id = pid; cohort = CohortMembershipDTO.EXITED } ->
            Choice1Of3 (ExitedPolicyId pid)
        | { policy_id = pid; cohort = CohortMembershipDTO.REMAINING } ->
            Choice2Of3 (RemainingPolicyId pid)
        | { policy_id = pid; cohort = CohortMembershipDTO.NEW } ->
            Choice3Of3 (NewPolicyId pid)

    let internal recordParser : DbDataReader -> _ =
        dtoRecordParser >> toUnderlying
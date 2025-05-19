
namespace AnalysisOfChangeEngine.DataStore.Postgres.DataTransferObjects

open System
open AnalysisOfChangeEngine.DataStore.Postgres


[<NoEquality; NoComparison>]
type internal StepHeaderDTO =
    {
        uid             : Guid
        title           : string
        description     : string
    }
    
[<RequireQualifiedAccess>]
module internal StepHeaderDTO =

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
        end

    let buildDispatcher connection =
        let dispatcher =
            new PostgresCommonTableDispatcher<StepHeaderDTO, Unit>
                ("step_headers", connection)

        let selectByUid =
            // Could we have used lazy instantiation here?
            // Although it would prevent code being generated unnecessarily,
            // the trade-off is that we could find issues occurring mid-flight.
            // It would be "best" if any issues occur as an exception
            // during assembly start-up code (ie... fail-safe AND fail-fast).
            dispatcher.MakeBaseEquality1Selector <@ _.uid @>

        {
            new IDispatcher with
                member _.SelectAll () =
                    dispatcher.SelectAllBaseRecords ()
                        
                member _.TryGetByUid (StepUid stepUid') =
                    match selectByUid stepUid' with
                    | []    -> None
                    | [x]   -> Some x
                    | _     -> failwithf "Multiple step headers found with UID %O." stepUid'
        }

    let toUnderlying (hdr: StepHeaderDTO): StepHeader =
        {
            Uid         = StepUid hdr.uid
            Title       = hdr.title
            Description = hdr.description            
        }

    let fromUnderlying (hdr: StepHeader): StepHeaderDTO =
        {
            uid         = hdr.Uid.Value
            title       = hdr.Title
            description = hdr.Description
        }


[<NoEquality; NoComparison>]
type internal ExtractionHeaderDTO =
    {
        uid             : Guid
        extraction_date : DateOnly
    }

[<RequireQualifiedAccess>]
module internal ExtractionHeaderDTO =

    type internal IDispatcher =
        interface
            abstract member SelectAll       : unit -> ExtractionHeaderDTO list
            abstract member TryGetByUid     : ExtractionUid -> ExtractionHeaderDTO option
            abstract member InsertRow       : ExtractionHeaderDTO -> unit
        end
        
    let buildDispatcher (schema, connection) =
        let dispatcher =
            new PostgresTableDispatcher<ExtractionHeaderDTO, Unit>
                ("extraction_headers", schema, connection)

        let selectByUid =
            dispatcher.MakeBaseEquality1Selector <@ _.uid @>

        let rowInserter =
            dispatcher.MakeRowInserter ()

        {
            new IDispatcher with
                member _.SelectAll () =
                    dispatcher.SelectAllBaseRecords ()

                member _.TryGetByUid (ExtractionUid extractionUid') =
                    match selectByUid extractionUid' with
                    | []    -> None
                    | [x]   -> Some x
                    | _     -> failwithf "Multiple extraction headers found with UID %O." extractionUid'

                member _.InsertRow row =
                    ignore <| rowInserter (row, ())
        }

    let toUnderlying (hdr: ExtractionHeaderDTO): ExtractionHeader =
        {
            Uid             = ExtractionUid hdr.uid
            ExtractionDate  = hdr.extraction_date            
        }

    let fromUnderlying (hdr: ExtractionHeader): ExtractionHeaderDTO =
        {
            uid             = hdr.Uid.Value
            extraction_date = hdr.ExtractionDate
        }


[<NoEquality; NoComparison>]
type internal PolicyDataBaseDTO =
    {
        extraction_uid          : Guid
        policy_id               : string        
    }

[<RequireQualifiedAccess>]
module internal PolicyDataDTO =

    type internal IDispatcher =
        interface
            abstract member GetPolicyIds     : ExtractionUid -> string Set
        end

    let internal builderDispatcher<'TAugRowDTO> (schema, connection) =
        let dispatcher =
            new PostgresTableDispatcher<PolicyDataBaseDTO, 'TAugRowDTO>
                ("policy_data", schema, connection)

        let policyIdGetter =
            dispatcher.MakeBaseEquality1Selector <@ _.extraction_uid @>
            >> Seq.map _.policy_id
            >> Set
                
        {
            new IDispatcher with

                member _.GetPolicyIds (ExtractionUid extractionUid') =
                    policyIdGetter extractionUid'
        }
            


[<NoEquality; NoComparison>]
type internal RunHeaderDTO =
    {
        uid                         : Guid
        title                       : string
        comments                    : string option
        created_by                  : string
        created_when                : DateTime
        opening_run_uid             : Guid option
        closing_run_date            : DateOnly
        policy_data_extraction_uid  : Guid        
    }

[<RequireQualifiedAccess>]
module internal RunHeaderDTO =

    type internal IDispatcher =
        interface
            abstract member SelectAll       : unit -> RunHeaderDTO list
            abstract member TryGetByUid     : RunUid -> RunHeaderDTO option
            abstract member InsertRow       : RunHeaderDTO -> unit
        end
        
    let buildDispatcher (schema, connection) =
        let dispatcher =
            new PostgresTableDispatcher<RunHeaderDTO, Unit>
                ("run_headers", schema, connection)

        let selectByUid =
            dispatcher.MakeBaseEquality1Selector <@ _.uid @>

        let rowInserter =
            dispatcher.MakeRowInserter ()

        {
            new IDispatcher with
                member _.SelectAll () =
                    dispatcher.SelectAllBaseRecords ()

                member _.TryGetByUid (RunUid runUid') =
                    match selectByUid runUid' with
                    | []    -> None
                    | [x]   -> Some x
                    | _     -> failwithf "Multiple run headers found with UID %O." runUid'

                member _.InsertRow row =
                    ignore <| rowInserter (row, ())
        }

    let toUnderlying (hdr: RunHeaderDTO): RunHeader =
        {
            Uid                         = RunUid hdr.uid
            Title                       = hdr.title
            Comments                    = hdr.comments
            CreatedBy                   = hdr.created_by
            CreatedWhen                 = hdr.created_when
            OpeningRunUid               = hdr.opening_run_uid |> Option.map RunUid 
            ClosingRunDate              = hdr.closing_run_date
            PolicyDataExtractionUid     = ExtractionUid hdr.policy_data_extraction_uid       
        }

    let fromUnderlying (hdr: RunHeader): RunHeaderDTO =
        {
            uid                         = hdr.Uid.Value
            title                       = hdr.Title
            comments                    = hdr.Comments
            created_by                  = hdr.CreatedBy
            created_when                = hdr.CreatedWhen
            opening_run_uid             = hdr.OpeningRunUid |> Option.map _.Value
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

    type internal IDispatcher =
        interface
            abstract member GetByRunUid     : RunUid -> RunStepDTO list
            abstract member InsertRow       : RunStepDTO -> unit
        end

    let buildDispatcher (schema, connection) =
        let dispatcher =
            new PostgresTableDispatcher<RunStepDTO, Unit>
                ("run_steps", schema, connection)

        let selectByRunUid =
            dispatcher.MakeBaseEquality1Selector <@ _.run_uid @>

        let rowInserter =
            dispatcher.MakeRowInserter ()

        {
            new IDispatcher with
                member _.GetByRunUid (RunUid runUid') =
                    selectByRunUid runUid'

                member _.InsertRow row =
                    ignore <| rowInserter (row, ())
        }


[<RequireQualifiedAccess; NoComparison; NoEquality>]
[<PostgresCommonEnumeration("validation_outcome")>]
type internal ValidationOutcomeDTO =
    | COMPLETED
    | FAILED
    | NOT_APPLICABLE

[<NoEquality; NoComparison>]
type internal StepResultsBaseDTO =
    {
        run_uid                 : Guid
        step_idx                : Int16
        used_data_stage_uid     : Guid
        validation_outcome      : ValidationOutcomeDTO
        run_when                : DateTime
        policy_id               : string
    }

[<RequireQualifiedAccess>]
module internal StepResultsDTO =
        
    type internal IDispatcher<'TAugRow> =
        interface
            // Having them curried in this way better reflects how this will be used.
            abstract member TryGetStepResults   : runUid: Guid -> stepIdx: Int16 -> policyId: string -> 'TAugRow option
        end

    let buildDispatcher<'TAugRowDTO> (schema, connection) =

        let dispatcher =
            PostgresTableDispatcher<StepResultsBaseDTO, 'TAugRowDTO>
                ("step_results", schema, connection)
            
        let resultGetter =
            dispatcher.MakeAugEquality3Selector (<@ _.run_uid @>, <@ _.step_idx @>, <@ _.policy_id @>)

        {
            new IDispatcher<'TAugRowDTO> with                    
                member _.TryGetStepResults runUid stepIdx policyId =
                    match resultGetter runUid stepIdx policyId with
                    | []    -> None
                    | [x]   -> Some x
                    | _     -> failwithf "Multiple step results found for step #%i and policy %s." stepIdx policyId
        }
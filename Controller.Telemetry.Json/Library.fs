
namespace AnalysisOfChangeEngine.Controller.Telemetry

open AnalysisOfChangeEngine


/// Provides some helper methods for formatting telemetry event data into
/// something that can be (more) easily serialized to JSON.
[<AbstractClass; Sealed>]
type JsonFormatter private () =
    
    // We need to use static members so we can channel requests
    // via function overloading.
    static member format (data: ApiRequestTelemetryEventData) =
        "api_request", {|
            requestor_name          = data.RequestorName
            data_source =
                match data.DataSource with
                | StepDataSource.OpeningData            -> None
                | StepDataSource.DataChangeStep step    -> Some step.Uid
                | StepDataSource.ClosingData            -> None
            policy_id               = data.PolicyId
            endpoint_id             = data.EndpointId
            request_submitted       = data.RequestSubmitted
            processing_start        = data.ProcessingStart
            processing_end          = data.ProcessingEnd
        |}

    static member format (data: RecordSubmittedTelemetryEventData) =
        "record_submitted", {|
            policy_id               = data.PolicyId
            timestamp               = data.Timestamp        
        |}

    static member format (data: PolicyReadTelemetryEventData) =
        "policy_read", {|
            policy_id               = data.PolicyId
            data_store_read_idx     = data.DataStoreReadIdx
            had_failures            = data.HadFailures
        |}

    static member format (data: EvaluationCompletedTelemetryEventData) =
        "evaluation_completed", {|
            policy_id               = data.PolicyId
            evaluation_start        = data.EvaluationStart
            evaluation_end          = data.EvaluationEnd
            had_failures            = data.HadFailures
        |}

    static member format (data: PolicyWriteTelemetryEventData) =
        "policy_write", {|
            policy_id               = data.PolicyId
            data_store_write_idx    = data.DataStoreWriteIdx
            had_failures            = data.HadFailures
        |}

    static member format (data: DataStoreReadEvent) =
        "data_store_read", {|
            idx                     = data.Idx
            read_start              = data.ReadStart
            read_end                = data.ReadEnd
        |}

    static member format (data: DataStoreWriteEvent) =
        "data_store_write", {|
            idx                     = data.Idx
            write_start             = data.WriteStart
            write_end               = data.WriteEnd
        |}

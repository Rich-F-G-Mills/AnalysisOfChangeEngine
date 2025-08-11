
namespace AnalysisOfChangeEngine.Controller.Telemetry

open AnalysisOfChangeEngine


[<AbstractClass; Sealed>]
type JsonFormatter private () =
    
    // We need to use static members so we can channel requests
    // via function overloading.
    static member format (data: ApiRequestTelemetryData) =
        {|
            event_type              = "api_request"
            policy_id               = data.PolicyId
            requestor_name          = data.RequestorName
            data_source =
                match data.DataSource with
                | StepDataSource.OpeningData            -> None
                | StepDataSource.DataChangeStep step    -> Some step.Uid
                | StepDataSource.ClosingData            -> None                
            endpoint_id             = data.EndpointId
            request_submitted       = data.RequestSubmitted
            processing_start        = data.ProcessingStart
            processing_end          = data.ProcessingEnd
        |}

    static member format (data: FailedPolicyReadTelemetryData) =
        {|
            event_type              = "failed_policy_read"
            policy_id               = data.PolicyId
            request_submitted       = data.RequestSubmitted
            data_store_read_idx     = data.DataStoreReadIdx
            data_store_write_idx    = data.DataStoreWriteIdx
        |}

    static member format (data: ProcessingCompletedTelemetryData) =
        {|
            event_type              = "processing_completed"
            policy_id               = data.PolicyId
            request_submitted       = data.RequestSubmitted
            evaluation_start        = data.EvaluationStart
            evaluation_end          = data.EvaluationEnd
            data_store_read_idx     = data.DataStoreReadIdx
            data_store_write_idx    = data.DataStoreWriteIdx
        |}

    static member format (data: DataStoreReadEvent) =
        {|
            event_type              = "data_store_read"
            idx                     = data.Idx
            read_start              = data.ReadStart
            read_end                = data.ReadEnd
        |}

    static member format (data: DataStoreWriteEvent) =
        {|
            event_type              = "data_store_write"
            idx                     = data.Idx
            write_start             = data.WriteStart
            write_end               = data.WriteEnd
        |}

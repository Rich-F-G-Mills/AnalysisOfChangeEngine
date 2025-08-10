
namespace AnalysisOfChangeEngine.Controller.Telemetry

open AnalysisOfChangeEngine
open AnalysisOfChangeEngine.Controller


[<RequireQualifiedAccess>]
module Json =
    
    let serialize (serializer: 'T -> string) = function
        | TelemetryEvent.ApiRequest data ->
            serializer {|
                event_type          = "api_request"
                policy_id           = data.PolicyId
                requestor_name      = data.RequestorName
                data_source =
                    match data.DataSource with
                    | StepDataSource.OpeningData            -> None
                    | StepDataSource.DataChangeStep step    -> Some step.Uid
                    | StepDataSource.ClosingData            -> None                
                endpoint_id         = data.EndpointId
                request_submitted   = data.RequestSubmitted
                processing_start    = data.ProcessingStart
                processing_end      = data.ProcessingEnd
            |}

        | TelemetryEvent.FailedPolicyRead data ->
            serializer {|
                event_type          = "failed_policy_read"
                policy_id           = data.PolicyId
                data_store_read_idx = data.DataStoreReadIdx
                data_store_write_idx = data.DataStoreWriteIdx
            |}
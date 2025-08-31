
namespace AnalysisOfChangeEngine.Controller.Telemetry

open System
open AnalysisOfChangeEngine


// Definitions of the various telemetry records as provided by the controller.

[<NoEquality; NoComparison>]
type ApiRequestTelemetryEventData =
    {
        PolicyId            : string
        RequestorName       : string
        DataSource          : StepDataSource
        EndpointId          : string option
        RequestSubmitted    : DateTime
        ProcessingStart     : DateTime
        ProcessingEnd       : DateTime
    }

[<NoEquality; NoComparison>]
type RecordSubmittedTelemetryEventData =
    {
        PolicyId            : string
        Timestamp           : DateTime
    }

[<NoEquality; NoComparison>]
type PolicyReadTelemetryEventData =
    {
        PolicyId            : string
        DataStoreReadIdx    : int
        HadFailures         : bool
    }

[<NoEquality; NoComparison>]
type EvaluationCompletedTelemetryEventData =
    {
        PolicyId            : string
        EvaluationStart     : DateTime
        EvaluationEnd       : DateTime
        HadFailures         : bool
    }

[<NoEquality; NoComparison>]
type PolicyWriteTelemetryEventData =
    {
        PolicyId            : string
        DataStoreWriteIdx   : int
        HadFailures         : bool
    }

[<NoEquality; NoComparison>]
type DataStoreReadEvent =
    {
        Idx                 : int
        ReadStart           : DateTime
        ReadEnd             : DateTime
    }

[<NoEquality; NoComparison>]
type DataStoreWriteEvent =
    {
        Idx                 : int
        WriteStart          : DateTime
        WriteEnd            : DateTime
    }


[<RequireQualifiedAccess>]
[<NoEquality; NoComparison>]
type TelemetryEvent =
    | ApiRequest            of ApiRequestTelemetryEventData
    | RecordSubmitted       of RecordSubmittedTelemetryEventData
    | PolicyRead            of PolicyReadTelemetryEventData
    | EvaluationCompleted   of EvaluationCompletedTelemetryEventData
    | PolicyWrite           of PolicyWriteTelemetryEventData
    | DataStoreRead         of DataStoreReadEvent
    | DataStoreWrite        of DataStoreWriteEvent

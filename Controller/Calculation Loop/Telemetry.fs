
namespace AnalysisOfChangeEngine.Controller.CalculationLoop

open System
open AnalysisOfChangeEngine


[<NoEquality; NoComparison>]
type ApiRequestTelemetryData =
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
type FailedPolicyReadTelemetryData =
    {
        PolicyId            : string
        DataStoreReadIdx    : int
        DataStoreWriteIdx   : int
    }

[<NoEquality; NoComparison>]
type EvaluationCompletedTelemetryData =
    {
        PolicyId            : string
        EvaluationStart     : DateTime
        EvaluationEnd       : DateTime
        DataStoreReadIdx    : int
        DataStoreWriteIdx   : int
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
    | ApiRequest            of ApiRequestTelemetryData
    | FailedPolicyRead      of FailedPolicyReadTelemetryData
    | EvaluationCompleted   of EvaluationCompletedTelemetryData
    | DataStoreRead         of DataStoreReadEvent
    | DataStoreWrite        of DataStoreWriteEvent
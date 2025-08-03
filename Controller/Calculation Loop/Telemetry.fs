
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
type EvaluationRequestTelemetryData =
    {
        PolicyId            : string
        RequestSubmitted    : DateTime
        ProcessingStart     : DateTime
        ProcessingEnd       : DateTime
        DataStoreReadIdx    : int
        DataStoreWriteIdx   : int
    }

[<NoEquality; NoComparison>]
type DataStoreReadEvent =
    {
        Idx                         : int
        ReadStart                   : DateTime
        ReadEnd                     : DateTime
        CountPolicyRecords          : int
        CountIndividualStepResults  : int
    }

[<NoEquality; NoComparison>]
type DataStoreWriteEvent =
    {
        Idx                         : int
        WriteStart                  : DateTime
        WriteEnd                    : DateTime
        CountDataStageRecords       : int
        CountIndividualStepResults  : int     
        CountFailureRecords         : int
    }


[<RequireQualifiedAccess>]
[<NoEquality; NoComparison>]
type TelemetryEvent =
    | ApiRequest        of ApiRequestTelemetryData
    | EvaluationRequest of EvaluationRequestTelemetryData
    | DataStoreRead     of DataStoreReadEvent
    | DataStoreWrite    of DataStoreWriteEvent
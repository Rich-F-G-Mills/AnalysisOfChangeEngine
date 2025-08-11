
namespace AnalysisOfChangeEngine.Controller.CalculationLoop

open System
open AnalysisOfChangeEngine
open AnalysisOfChangeEngine.Controller


[<RequireQualifiedAccess>]
[<NoEquality; NoComparison>]
type internal CohortedPolicyId =
    | Exited    of string
    | Remaining of string
    | New       of string

    member this.Underlying =
        match this with
        | Exited pid    -> pid
        | Remaining pid -> pid
        | New pid       -> pid

[<RequireQualifiedAccess>]
[<NoEquality; NoComparison>]
type internal CohortedPolicyRecord<'TPolicyRecord, 'TStepResults> =
    | Exited    of Record: 'TPolicyRecord * PriorClosingResults: 'TStepResults option
    | Remaining of Opening: 'TPolicyRecord * Closing: 'TPolicyRecord * PriorClosingResults: 'TStepResults option
    | New       of 'TPolicyRecord

[<NoEquality; NoComparison>]
type internal PolicyEvaluationRequest<'TPolicyRecord, 'TStepResults> =
    {
        PolicyId            : CohortedPolicyId
        PolicyRecord        : Result<CohortedPolicyRecord<'TPolicyRecord, 'TStepResults>, PolicyReadFailure list>
        RequestSubmitted    : DateTime
        DataReadIdx         : int
    }

[<NoEquality; NoComparison>]
type internal OutputCompletedEvaluationRequest<'TPolicyRecord, 'TStepResults> =
    {
        PolicyId            : CohortedPolicyId
        RequestSubmitted    : DateTime
        EvaluationStart     : DateTime
        EvaluationEnd       : DateTime
        WalkOutcome         : Result<EvaluatedPolicyWalk<'TPolicyRecord, 'TStepResults>, WalkEvaluationFailure list>
        DataReadIdx         : int
    }

[<NoEquality; NoComparison>]
type internal OutputFailedPolicyReadRequest<'TPolicyRecord, 'TStepResults> =
    {
        PolicyId            : CohortedPolicyId
        RequestSubmitted    : DateTime
        FailureReasons      : PolicyReadFailure list
        DataReadIdx         : int
    }

[<RequireQualifiedAccess>]
[<NoEquality; NoComparison>]
type internal OutputRequest<'TPolicyRecord, 'TStepResults> =
    | CompletedEvaluation of OutputCompletedEvaluationRequest<'TPolicyRecord, 'TStepResults>
    | FailedPolicyRead    of OutputFailedPolicyReadRequest<'TPolicyRecord, 'TStepResults>


namespace AnalysisOfChangeEngine.Controller.CalculationLoop


[<AutoOpen>]
module internal Common =

    open System
    open AnalysisOfChangeEngine


    [<RequireQualifiedAccess>]
    [<NoEquality; NoComparison>]
    type internal PolicyId =
        | Exited    of string
        | Remaining of string
        | New       of string

        member this.PolicyId =
            match this with
            | Exited pid    -> pid
            | Remaining pid -> pid
            | New pid       -> pid

    [<RequireQualifiedAccess>]
    [<NoEquality; NoComparison>]
    type internal PolicyRecord<'TPolicyRecord, 'TStepResults> =
        | Exited    of Record: 'TPolicyRecord * PriorClosingResults: 'TStepResults option
        | Remaining of Opening: 'TPolicyRecord * Closing: 'TPolicyRecord * PriorClosingResults: 'TStepResults option
        | New       of 'TPolicyRecord


    /// Reflects a case that has been into the loop but has yet to be read from the underlying data-store.
    [<NoEquality; NoComparison>]
    type internal PendingReadRequest =
        {
            PolicyId            : PolicyId
            RequestSubmitted    : DateTime
        }


    /// Reflects a case where the underlying data has been read from the data-store,
    /// but we have yet to evaluate it.
    [<NoEquality; NoComparison>]
    type internal PendingEvaluationRequest<'TPolicyRecord, 'TStepResults> =
        {
            PolicyId            : PolicyId
            PolicyRecord        : PolicyRecord<'TPolicyRecord, 'TStepResults>
            RequestSubmitted    : DateTime
        }

    [<RequireQualifiedAccess>]
    [<NoEquality; NoComparison>]
    type internal PendingEvaluationRequestFailure =
        | OpeningRecordNotFound
        | ClosingRecordNotFound
        | OpeningRecordParseFailure of Reasons: string array
        | ClosingRecordParseFailure of Reasons: string array

    type internal PendingEvaluationRequestOutcome<'TPolicyRecord, 'TStepResults> =
        Result<PendingEvaluationRequest<'TPolicyRecord, 'TStepResults>, PendingEvaluationRequestFailure list>

    type internal ProcessedRecordOutcome<'TPolicyRecord, 'TStepResults> =
        Result<EvaluatedPolicyWalk<'TPolicyRecord, 'TStepResults>, EvaluationFailure list>


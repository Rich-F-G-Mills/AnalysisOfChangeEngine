
namespace AnalysisOfChangeEngine

open System


type ILogger =
    interface
        abstract member LogInfo     : message: string -> unit
        abstract member LogDebug    : message: string -> unit
        abstract member LogWarning  : message: string -> unit
        abstract member LogError    : message: string -> unit
    end


/// Required interface for all step types.
type IStepHeader =
    interface
        abstract member Uid         : Guid with get
        abstract member Title       : string with get
        abstract member Description : string with get
    end


[<RequireQualifiedAccess>]
[<NoEquality; NoComparison>]
type StepDataSource =
    | OpeningData
    | ClosingData
    // Theoretically, we should use the data changer interface. However,
    // given it's generic with respect to the policy record type, we'd
    // then need to make this generic as well.
    | DataChangeStep of IStepHeader


[<NoEquality; NoComparison>]
type ExitedPolicy<'TPolicyRecord> =
    | ExitedPolicy of Opening: 'TPolicyRecord

    member this.PolicyRecord =
        match this with
        | ExitedPolicy policyRecord ->
            policyRecord

[<NoEquality; NoComparison>]
type RemainingPolicy<'TPolicyRecord> =
    | RemainingPolicy of Opening: 'TPolicyRecord * Closing: 'TPolicyRecord

    member this.PolicyRecords =
        match this with
        | RemainingPolicy (openingPolicyRecord, closingPolicyRecord) ->
            openingPolicyRecord, closingPolicyRecord                
        
[<NoEquality; NoComparison>]
type NewPolicy<'TPolicyRecord> =
    | NewPolicy of Closing: 'TPolicyRecord

    member this.PolicyRecord =
        match this with
        | NewPolicy policyRecord ->
            policyRecord


[<NoEquality; NoComparison>]
type ExitedPolicyId =
    | ExitedPolicyId of string
        
    member this.Value =
        match this with
        | ExitedPolicyId id -> id

[<NoEquality; NoComparison>]
type RemainingPolicyId =
    | RemainingPolicyId of string
        
    member this.Value =
        match this with
        | RemainingPolicyId id -> id

[<NoEquality; NoComparison>]
type NewPolicyId =
    | NewPolicyId of string
        
    member this.Value =
        match this with
        | NewPolicyId id -> id


namespace AnalysisOfChangeEngine

open System
open FSharp.Quotations
open AnalysisOfChangeEngine.Common


[<AbstractClass>]
// Private constructor as this will NEVER be instantiated. It purely provides a container
// for actions that can be performed as part of a source definition. Furthermore, having them
// defined as instance members allows them to be more easily discoverable via reflection.
type SourceAction<'TPolicyRecord, 'TStepResults, 'TApiCollection> private () =
    (*
    Design Decision:
        Could have used a curried call rather than tupled; however, identifying (let along deciphering)
        it within any code quotation where it was used was proving a lot more complicated.

        Furthermore... Why would we not just directly pass in the wrapped API requestor?
        Firstly, we are limited what can be passed in to a code quotation. When we
        compile the walk, we pass in a collection of API requestors. We can therefore
        provide a mapping from this collection to one of the requestor within. Put another
        way, it is trivial to quote the accessing of an object's property.
    *)
    /// Request specific output from the referenced API end-point and corresponding response.        
    abstract member apiCall<'TResponse, 'T>
        : apiRequest: ('TApiCollection -> WrappedApiRequestor<'TPolicyRecord, 'TResponse>) * selector: ('TResponse -> 'T) -> 'T


/// Required type of all source definitions for the opening re-run step.
type OpeningReRunSourceExpr<'TPolicyRecord, 'TStepResults, 'TApiCollection> =
    Expr<SourceAction<'TPolicyRecord, 'TStepResults, 'TApiCollection>
        -> 'TPolicyRecord   // Current policy record.
        -> 'TStepResults    // Current results.
        -> 'TStepResults>   // Constructed results for current step.

[<RequireQualifiedAccess>]
module private OpeningReRunSourceExpr =
    // Used to break apart a source definition.
    let internal (|Definition|_|) (sourceExpr: OpeningReRunSourceExpr<_, 'TStepResults, _>) =
        match sourceExpr with
        | Patterns.Lambda (from,
            Patterns.Lambda (policyRecord,
                    Patterns.Lambda (currentResults, sourceBody))) ->
                        let sourceBody' =
                            Expr.Cast<'TStepResults> sourceBody

                        Some (from, policyRecord, currentResults, sourceBody')
        | _ ->
            None


/// Required type of all source definitions (except for the opening re-run step).
type SourceExpr<'TPolicyRecord, 'TStepResults, 'TApiCollection> =
    Expr<SourceAction<'TPolicyRecord, 'TStepResults, 'TApiCollection>
        -> 'TPolicyRecord   // Current policy record.
        -> 'TStepResults    // Prior source definition.
        -> 'TStepResults    // Current source definition.
        -> 'TStepResults>   // Constructed results for current step.

[<RequireQualifiedAccess>]
module SourceExpr =
    /// Not intended for direct developer use.
    let cast<'TPolicyRecord, 'TStepResults, 'TApiCollection> expr
        : SourceExpr<'TPolicyRecord, 'TStepResults, 'TApiCollection> =
            Expr.Cast<_> expr

    // Used to break apart a source definition.
    /// Not intended for direct developer use.
    let (|Definition|_|) (sourceExpr: SourceExpr<_, 'TStepResults, _>) =
        match sourceExpr with
        | Patterns.Lambda (from,
            Patterns.Lambda (policyRecord,
                Patterns.Lambda (priorResults,
                    Patterns.Lambda (currentResults, sourceBody)))) ->
                        let sourceBody' =
                            Expr.Cast<'TStepResults> sourceBody

                        Some (from, policyRecord, priorResults, currentResults, sourceBody')
        | _ ->
            None


(*
Design Decision:
    Why not just use a Result type for this?
    Although a fair question... What would 'Ok' mean specifically? Using this specific DU,
    we make it clear that we care more about whether the validation completed or not.
*)
/// Represents the outcome of step validation logic.
[<RequireQualifiedAccess; NoEquality; NoComparison>]
type StepValidationOutcome =
    /// Indicates that the validation logic was successfully applied without any issues.
    | Completed
    /// Indicates that the validation logic was successfully applied with resulting issues.
    | CompletedWithIssues   of Reasons: string nonEmptyList
    /// Indicates that the validation logic was unable to run for a specified reason.
    | Aborted               of Reason: string

    /// Helper function that provides no validation at all.
    static member noValidator _ : StepValidationOutcome =
        StepValidationOutcome.Completed

/// Step validator that receives the current policy record followed by
/// the prior (where available) and current step results.
type OpeningReRunStepValidator<'TPolicyRecord, 'TStepResults> =
    'TPolicyRecord * 'TStepResults option * 'TStepResults -> StepValidationOutcome

/// Step validator that receives the prior policy record and step results,
/// followed by those for the current step. Note that this will ONLY be run
/// if a data change has actually occurred at this step.
type DataChangeStepValidator<'TPolicyRecord, 'TStepResults> =
    'TPolicyRecord * 'TStepResults * 'TPolicyRecord * 'TStepResults -> StepValidationOutcome

/// Step validator that receives the current policy record followed by
/// the prior  and current step results.
type SourceChangeStepValidator<'TPolicyRecord, 'TStepResults> =
    'TPolicyRecord * 'TStepResults * 'TStepResults -> StepValidationOutcome

/// Step validator that receives the new policy record and corresponding step results.
type AddNewRecordsStepValidator<'TPolicyRecord, 'TStepResults> =
    'TPolicyRecord * 'TStepResults -> StepValidationOutcome


/// Type definition for a data changer that takes the policy record as at the opening step,
/// prior step and closing step. Return None if no record change is required. If a Some value
/// is returned, it is always assumed that a data change has occurred.
type PolicyRecordChanger<'TPolicyRecord> =
    // Opening * Prior * Closing -> Optional Revised
    'TPolicyRecord * 'TPolicyRecord * 'TPolicyRecord -> Result<'TPolicyRecord option, string nonEmptyList>


/// Required interface for any step that has a source definition.
type ISourceableStep<'TPolicyRecord, 'TStepResults, 'TApiCollection> =
    interface
        inherit IStepHeader
        abstract member Source: SourceExpr<'TPolicyRecord, 'TStepResults, 'TApiCollection>
    end


/// Required interface for steps that can modify policy records. This does NOT
/// relate to steps which change whether a policy record is included (or not).
type IDataChangeStep<'TPolicyRecord> =
    interface
        inherit IStepHeader
        abstract member DataChanger: PolicyRecordChanger<'TPolicyRecord> with get
    end


/// Required first step. In the absence of (for example) model changes or
/// errors in the underlying logic, this would be expected to tie up
/// the prior run's closing position.
[<NoEquality; NoComparison>]
type OpeningReRunStep<'TPolicyRecord, 'TStepResults, 'TApiCollection> =
    {
        Uid             : Guid
        Title           : string
        Description     : string
        Source          : OpeningReRunSourceExpr<'TPolicyRecord, 'TStepResults, 'TApiCollection>
        Validator       : OpeningReRunStepValidator<'TPolicyRecord, 'TStepResults>
    }

    interface IStepHeader with
        member this.Uid = this.Uid
        member this.Title = this.Title
        member this.Description = this.Description

    interface ISourceableStep<'TPolicyRecord, 'TStepResults, 'TApiCollection> with
        member this.Source =
            match this.Source with
            | OpeningReRunSourceExpr.Definition
                (from, policyRecord, currentResults, sourceBody) ->
                    let untypedExpr =
                        Expr.Lambda (from,
                            Expr.Lambda (policyRecord,
                                // This is solely to make sure our lambda has the correct signature.
                                Expr.Lambda (Var ("priorResults", typeof<'TStepResults>),
                                    Expr.Lambda (currentResults, sourceBody))))

                    SourceExpr.cast<_, _, _> untypedExpr

            | _ ->
                failwith "Invalid source defintion."


(*
Design decision:
    Arguably, do we need to have a separate step for this? One alternative would be for
    us to just "know" that exited policies are removed after the opening re-run step.
    However, having a dedicated step for this makes it explicit.
    We have a similar situation later with the add new records step.
*)
/// Indicates that policies not present in the closing position are to be excluded
/// from further processing.
[<NoEquality; NoComparison>]
type RemoveExitedRecordsStep<'TPolicyRecord, 'TStepResults> =
    {
        Uid             : Guid
        Title           : string
        Description     : string
    }

    interface IStepHeader with
        member this.Uid = this.Uid
        member this.Title = this.Title
        member this.Description = this.Description 


/// Indicates a step where the source can be specified.
[<NoEquality; NoComparison>]
type SourceChangeStep<'TPolicyRecord, 'TStepResults, 'TApiCollection> =
    {
        Uid             : Guid
        Title           : string
        Description     : string
        Source          : SourceExpr<'TPolicyRecord, 'TStepResults, 'TApiCollection>
        Validator       : SourceChangeStepValidator<'TPolicyRecord, 'TStepResults>
    }

    interface IStepHeader with
        member this.Uid = this.Uid
        member this.Title = this.Title
        member this.Description = this.Description    

    interface ISourceableStep<'TPolicyRecord, 'TStepResults, 'TApiCollection> with
        member this.Source =
            this.Source
                

/// Indicates a step where the current policy record can be changed in a specified way.
/// This cannot change whether a policy is included (or not) for processing.
[<NoEquality; NoComparison>]
type DataChangeStep<'TPolicyRecord, 'TStepResults> =
    {
        Uid             : Guid
        Title           : string
        Description     : string
        DataChanger     : PolicyRecordChanger<'TPolicyRecord>
        Validator       : DataChangeStepValidator<'TPolicyRecord, 'TStepResults>
    }

    interface IStepHeader with
        member this.Uid = this.Uid
        member this.Title = this.Title
        member this.Description = this.Description 

    interface IDataChangeStep<'TPolicyRecord> with
        member this.DataChanger = this.DataChanger             


/// Underlying type of the penultimate (and required) step in the walk.
/// Indicates a move to the closing data for a given policy record.
[<NoEquality; NoComparison>]
// We need the equality constraint here as we need to be able to compare closing policy
// records against those inherited from the prior step.
type MoveToClosingDataStep<'TPolicyRecord, 'TStepResults when 'TPolicyRecord: equality> =
    {
        Uid             : Guid
        Title           : string
        Description     : string
        Validator       : DataChangeStepValidator<'TPolicyRecord, 'TStepResults>
    }

    interface IStepHeader with
        member this.Uid = this.Uid
        member this.Title = this.Title
        member this.Description = this.Description

    interface IDataChangeStep<'TPolicyRecord> with
        member _.DataChanger =
            fun (_, prior, closing) ->
                if prior = closing then
                    // If there's no change in the policy record, then indicate as such.
                    Ok None
                else
                    // Conversely, return the modified record.
                    Ok (Some closing)


(*
Design Decision:
    As with the remove exited records step, do we need a seperate step for this?
    Could we not just have them "appear" as part of the move to closing data step?
    Firstly, new records would have a different validation function signature compared
    to existing records. Again, having a dedicated step makes it explicit; the user
    doesn't need to remember that new records magically appear as part of a different step.
*)
/// Underlying type of the final (and required) step where policies only
/// present in the closing position are valued.
[<NoEquality; NoComparison>]
type AddNewRecordsStep<'TPolicyRecord, 'TStepResults> =
    {
        Uid             : Guid
        Title           : string
        Description     : string
        Validator       : AddNewRecordsStepValidator<'TPolicyRecord, 'TStepResults>
    }

    interface IStepHeader with
        member this.Uid = this.Uid
        member this.Title = this.Title
        member this.Description = this.Description

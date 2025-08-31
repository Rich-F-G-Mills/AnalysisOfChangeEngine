
namespace AnalysisOfChangeEngine

open System
open System.Reflection
open System.Threading.Tasks
open AnalysisOfChangeEngine.Common


// Again, we use that same strong typing approach here.

[<NoEquality; NoComparison>]
type RequestorName =
    | RequestorName of string

[<NoEquality; NoComparison>]
type EndpointId =
    | EndpointId of string


type OnApiRequestProcessingStart =
    // Potentially, there may be some cases where an end-point ID
    // is meaningless. As such, have made it optional.
    EndpointId option -> IDisposable


[<RequireQualifiedAccess>]
[<NoEquality; NoComparison>]
type ApiRequestFailure =
    /// Indicates that, although a response was received from the API,
    /// it failed either in part or in its entirety.
    | CalculationFailure of Reasons: string nonEmptyList
    /// Indicates that an API call failed to execute. This would,
    /// for example, be the case if the API was not available.
    | CallFailure of Reasons: string nonEmptyList

/// Represents the outcome of an asynchronous execution
/// performed by an IApiRequestor object,
type ApiRequestOutcome =
    // Given we will want to directly index into the objects returned by the API,
    // an array is preferable to a list.
    Result<obj array, ApiRequestFailure>


(*
Design Decision:
    Why not use an interface instead?
    We want to use the instances for grouping operations. By using an
    abstract class, we can bake-in implementations for eqality and
    comparison operations.

    Is it risky to only consider the Name for this operations?
    TODO - Could reference equality be used for grouping oeprations?

    Do NOT change this to IApiEndpoint. Again! The same endpoint could have
    multiple requestors mapping to it. Calling it 'endpoint' would be misleading.
    Furthermore, a single requestor could be using multiple endpoints behind
    the scenes.
*)

/// Types implementing this interface provide an asynchronous way to submit a
/// calculation request to an API end-point.
[<AbstractClass>]
type AbstractApiRequestor<'TPolicyRecord> () =
    /// Note that API requestors with the SAME NAME will be grouped together!
    abstract member Name: string with get

    /// Asyncronously submit a calculation request to the underlying end-point.
    abstract member ExecuteAsync:
        PropertyInfo array
            // Given the policy record and processing callback are inexorably linked,
            // they should be supplied together as a tuple.
            -> 'TPolicyRecord * OnApiRequestProcessingStart
            -> Task<ApiRequestOutcome>
        
    override this.Equals other =
        match other with
        | :? AbstractApiRequestor<'TPolicyRecord> as other' ->
            (this :> IEquatable<_>).Equals other'
        | _ ->
            false

    override this.GetHashCode () =
        this.Name.GetHashCode ()

    interface IEquatable<AbstractApiRequestor<'TPolicyRecord>> with
        member this.Equals (other) =
            this.Name = other.Name

    interface IComparable<AbstractApiRequestor<'TPolicyRecord>> with
        member this.CompareTo other =
            this.Name.CompareTo other.Name

    interface IComparable with
        member this.CompareTo other =
            match other with
            | :? AbstractApiRequestor<'TPolicyRecord> as other' ->
                (this :> IComparable<_>).CompareTo other'
            | _ ->
                failwith "Cannot compare different types of API requestors."


[<RequireQualifiedAccess>]
/// Contains logic providing for the creation of API requestors.
module ApiRequestor =
        
    [<Sealed>]
    // No-one, and I mean no-one, should be deriving from this.
    /// Not intended for direct developer use.
    type EncapsulatedApiRequestor<'TPolicyRecord> internal (name, asyncExecutor) =
        inherit AbstractApiRequestor<'TPolicyRecord> ()

        override _.Name =
            name

        override _.ExecuteAsync requiredOutputs  (policyRecord, onApiRequestProcessingStart) =
            asyncExecutor requiredOutputs (policyRecord, onApiRequestProcessingStart)

    /// Create an API requestor using the supplied name and executor logic.
    let create (name, asyncExecutor) : AbstractApiRequestor<'TPolicyRecord> =
        upcast new EncapsulatedApiRequestor<'TPolicyRecord> (name, asyncExecutor)


// This effectively allows us to peel away the generic API
// response type from a wrapped API requestor.
/// Not intended for direct developer use.
type IWrappedApiRequestor<'TPolicyRecord> =
    interface
        abstract member UnderlyingRequestor:
            AbstractApiRequestor<'TPolicyRecord> with get
    end


(*
Design Decision:
    We also need to track the possible API responses... We cannot use
    a vanilla type alias as it will complain about TResponse not actually getting used.
    However, we have no such issue when using a DU. Ultimately, this is just an API requestor
    with some supplementary type information for intelli-sense purposes.
*)
/// Represents an API requestor which, via a "baked-in" generic type-parameter, provides
/// a strongly typed link to possible responses.
[<NoEquality; NoComparison>]
type WrappedApiRequestor<'TPolicyRecord, 'TResponse> =
    | WrappedApiRequestor of AbstractApiRequestor<'TPolicyRecord>

    // As mentioned above, allows us to peel away the generic response type
    // which we don't _actually_ care about.
    interface IWrappedApiRequestor<'TPolicyRecord> with

        member this.UnderlyingRequestor =
            match this with
            | WrappedApiRequestor abstractApiRequestor ->
                abstractApiRequestor

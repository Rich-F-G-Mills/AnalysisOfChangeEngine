
namespace AnalysisOfChangeEngine.Implementations.OBWholeOfLife

open System

open AnalysisOfChangeEngine


[<RequireQualifiedAccess; NoComparison>]
type Gender =
    | Male
    | Female

[<RequireQualifiedAccess>]
module internal Gender =
    let createParser (male, female) (value: 'T) =
        if value = male then
            Ok Gender.Male
        elif value = female then
            Ok Gender.Female
        else
            Error value

[<RequireQualifiedAccess; NoComparison>]
type PolicyStatus =
    | PaidUp
    | AllPaid
    | PremiumPaying

[<RequireQualifiedAccess>]
module PolicyStatus =
    let createParser (pp, pup, ap) (value: 'T) =
        if value = pp then
            Ok PolicyStatus.PremiumPaying
        elif value = pup then
            Ok PolicyStatus.PaidUp
        elif value = ap then
            Ok PolicyStatus.AllPaid
        else
            Error value

[<NoComparison>]
type LifeData =
    {
        EntryAge: int
        Gender: Gender
    }

[<NoComparison>]
type LivesBasis =
    | SingleLife of Life1: LifeData
    | JointLife of Life1: LifeData * Life2: LifeData * JointValuationAge: int

    member this.EntryAgeLife1 =
        match this with
        | SingleLife { EntryAge = ea }
        | JointLife ({ EntryAge = ea }, _, _) -> ea

    member this.GenderLife1 =
        match this with
        | SingleLife { Gender = g }
        | JointLife ({ Gender = g }, _, _) -> g

    member this.EntryAgeLife2 =
        match this with
        | JointLife (_, { EntryAge = ea }, _) -> Some ea
        | _ -> None

    member this.GenderLife2 =
        match this with
        | JointLife (_, { Gender = g }, _) -> Some g
        | _ -> None

    member this.JointValuationAge =
        match this with
        | JointLife (_, _, jva) -> Some jva
        | _ -> None


// By the time this has been populated, all required cleansing has been performed.
// This is independent of the underlyine source.
// Wherever the data is sourced from, it MUST ultimately end up as the following.
[<NoComparison>]
type PolicyRecord =
    {
        PolicyNumber        : string
        TableCode           : string
        EntryDate           : DateOnly
        NextPremiumDueDate  : DateOnly
        PolicyStatus        : PolicyStatus
        LivesBasis          : LivesBasis
        PaymentTerm         : int
        SumAssured          : double
    }

    interface IPolicyRecord with
        member this.ID = this.PolicyNumber


[<RequireQualifiedAccess>]
module PolicyRecord =

    // This is intentionally independent of whatever mechanism actually created the
    // policy record to begin with.
    let validate (r: PolicyRecord) =
        [
            if r.NextPremiumDueDate < r.EntryDate then
                yield "NPDD cannot be before entry date."

            if r.PaymentTerm < 0 then
                yield "Payment term must be positive." 

            if r.LivesBasis.EntryAgeLife1 < 0 then
                yield "Life 1 entry age must be non-negative."

            match r.LivesBasis.EntryAgeLife2 with
            | Some age when age < 0 ->
                yield "Life 2 entry age must be non-negative."
            | _ ->
                do ()

            match r.LivesBasis.JointValuationAge with
            | Some jva when jva < 0 ->
                yield "Joint-valuation age must be non-negative."
            | _ ->
                do ()
        ]
         
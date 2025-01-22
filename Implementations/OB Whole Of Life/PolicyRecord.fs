
namespace AnalysisOfChangeEngine.Implementations.OBWholeOfLife

open System

open AnalysisOfChangeEngine.Implementations


[<RequireQualifiedAccess>]
[<NoComparison>]
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

[<RequireQualifiedAccess>]
[<NoComparison>]
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

[<NoEquality; NoComparison>]
type LifeData =
    {
        EntryAge: int
        Gender: Gender
    }

[<NoEquality; NoComparison>]
type LivesBasis =
    | SingleLife of Life1: LifeData
    | JointLife of Life1: LifeData * Life2: LifeData * JointValuationAge: int

// By the time this has been populated, all required cleansing has been performed.
// This is independent of the underlyine source.
// Wherever the data is sourced from, it must ultimately end up as the following.
[<NoEquality; NoComparison>]
type PolicyRecord =
    {
        PolicyNumber        : string
        EntryDate           : DateOnly
        NextPremiumDueDate  : DateOnly
        PolicyStatus        : PolicyStatus
        LivesBasis          : LivesBasis
        PaymentTerm         : int
    }

    interface IPolicyRecord with
        member this.ID = this.PolicyNumber
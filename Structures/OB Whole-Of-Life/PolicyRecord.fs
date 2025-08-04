
namespace AnalysisOfChangeEngine.Structures.PolicyRecords


[<RequireQualifiedAccess>]
module OBWholeOfLife =

    open System


    [<RequireQualifiedAccess; NoComparison>]
    type Gender =
        | Male
        | Female

    [<RequireQualifiedAccess; NoComparison>]
    type PolicyStatus =
        | PaidUp
        | AllPaid
        | PremiumPaying

    [<RequireQualifiedAccess; NoComparison>]
    type PremiumFrequency =
        | Monthly
        | Yearly

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
    type RawPolicyRecord =
        {
            TableCode           : string
            EntryDate           : DateOnly
            NextPremiumDueDate  : DateOnly
            PremiumFrequency    : PremiumFrequency
            ModalPremium        : float32
            Status              : PolicyStatus
            Lives               : LivesBasis
            LimitedPaymentTerm  : int
            SumAssured          : float32
            Taxable             : bool
        }


    [<NoComparison>]
    type PolicyRecord =
        // Restrict the construction of this type to the validation function.
        private | PolicyRecord of RawPolicyRecord


    // Allows the inner policy record to be accessed via pattern matching.
    let (|PolicyRecord|) (PolicyRecord pr) =
        pr


    [<RequireQualifiedAccess>]
    module PolicyRecord =

        // This is intentionally independent of whatever mechanism actually created the
        // policy record to begin with.
        let validate (r: RawPolicyRecord) =
            let validationFailures =
                [
                    if r.NextPremiumDueDate < r.EntryDate then
                        yield " * NPDD cannot be before entry date."

                    if r.LimitedPaymentTerm < 0 then
                        yield " * Payment term must be positive." 

                    if r.Lives.EntryAgeLife1 < 0 then
                        yield " * Life 1 entry age must be non-negative."

                    match r.Lives.EntryAgeLife2 with
                    | Some age when age < 0 ->
                        yield " * Life 2 entry age must be non-negative."
                    | _ ->
                        do ()

                    match r.Lives.JointValuationAge with
                    | Some jva when jva < 0 ->
                        yield " * Joint-valuation age must be non-negative."
                    | _ ->
                        do ()
                ]

            match validationFailures with
            | []    -> Ok (PolicyRecord r)
            | _     -> Error validationFailures
         

namespace AnalysisOfChangeEngine.Walks


[<RequireQualifiedAccess>]
module PxApi =

    open System
    open AnalysisOfChangeEngine
    open AnalysisOfChangeEngine.Structures.PolicyRecords
    open AnalysisOfChangeEngine.Structures.StepResults


    type private PolicyRecord = OBWholeOfLife.PolicyRecord
    type private Gender = OBWholeOfLife.Gender
    type private LivesBasis = OBWholeOfLife.LivesBasis


    [<RequireQualifiedAccess; NoEquality; NoComparison>]
    type PxGender =
        | M
        | F

    [<RequireQualifiedAccess; NoEquality; NoComparison>]
    type PxLivesBasis =
        | S
        | J


    [<NoComparison>]
    type StepRelatedAttributes =
        {
            AoCOpeningCalculationDate   : DateOnly option
            CalculationDate             : DateOnly        
        }

    [<NoComparison>]
    type PolicyRelatedAttributes =
        {
            EntryDate                   : DateOnly
            NextPremiumDueDate          : DateOnly
            EntryAgeLife1               : int
            GenderLife1                 : PxGender
            EntryAgeLife2               : int option
            JointValuationAge           : int option
            SingleJointLife             : PxLivesBasis
        }


    [<NoEquality; NoComparison>]
    type OutputAttributes =
        {
            CashSurrenderValue              : double
            DeathBenefit                    : double
            UnsmoothedAssetShare            : double
            SmoothedAssetShare              : double
            ExitBonusRate                   : double
            UnpaidPremiums                  : double
            GuaranteedDeathBenefit          : double
            DeathUpliftFactor               : double

            Step0_Opening_UAS               : double
            Step0_Opening_SAS               : double
            Step1_RestatedAdjustments_UAS   : double
            Step1_RestatedAdjustments_SAS   : double
            Step2_RestatedActuals_UAS       : double
            Step2_RestatedActuals_SAS       : double
            Step3_RestatedDeductions_UAS    : double
            Step3_RestatedDeductions_SAS    : double
            Step4_MoveToClosingDate_UAS     : double
            Step4_MoveToClosingDate_SAS     : double
            Step5_Adjustments_UAS           : double
            Step5_Adjustments_SAS           : double
            Step6_Premiums_UAS              : double
            Step6_Premiums_SAS              : double
            Step7_Deductions_UAS            : double
            Step7_Deductions_SAS            : double
            Step8_MortalityCharge_UAS       : double
            Step8_MortalityCharge_SAS       : double
            Step9_InvestmentReturn_UAS      : double
            Step9_InvestmentReturn_SAS      : double
        }


    let getPolicyRelatedAttributes (r: PolicyRecord) : PolicyRelatedAttributes =
        {
            EntryDate =
                r.EntryDate
            NextPremiumDueDate =
                r.NextPremiumDueDate
            EntryAgeLife1 =
                r.Lives.EntryAgeLife1
            GenderLife1 =
                match r.Lives.GenderLife1 with
                | Gender.Male   -> PxGender.M
                | Gender.Female -> PxGender.F
            EntryAgeLife2 =
                r.Lives.EntryAgeLife2
            JointValuationAge =
                r.Lives.JointValuationAge
            SingleJointLife =
                match r.Lives with
                | LivesBasis.SingleLife _ -> PxLivesBasis.S
                | LivesBasis.JointLife _  -> PxLivesBasis.J  
        }


    [<NoEquality; NoComparison>]
    type OpeningDispatcherConfig =
        {
            OpeningRunDate: DateOnly
        }


    [<NoEquality; NoComparison>]
    type PostOpeningDispatcherConfig =
        {
            OpeningRunDate: DateOnly
            ClosingRunDate: DateOnly
        }


    let createOpeningDispatcher<'TPolicyRecord>
        (config: OpeningDispatcherConfig)
        : WrappedApiRequestor<'TPolicyRecord, OutputAttributes> =
            WrappedApiRequestor {
                new IApiRequestor<_> with
                    member _.Name =
                        "PX API [Opening]"
                    member _.Execute field record =
                        Error "failed"
            }


    let createPostOpeningDispatcher<'TPolicyRecord>
        (config: PostOpeningDispatcherConfig)
        : WrappedApiRequestor<'TPolicyRecord, OutputAttributes> =
            WrappedApiRequestor {
                new IApiRequestor<_> with
                    member _.Name =
                        "PX API [Post-Opening Regression]"
                    member _.Execute field record =
                        Error "failed"
            }
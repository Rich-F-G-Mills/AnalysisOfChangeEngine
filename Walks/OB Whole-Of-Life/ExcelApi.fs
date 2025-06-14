
namespace AnalysisOfChangeEngine.Walks


[<RequireQualifiedAccess>]
module ExcelApi =

    open System
    open FsToolkit.ErrorHandling
    open AnalysisOfChangeEngine
    open AnalysisOfChangeEngine.Structures.PolicyRecords
    open AnalysisOfChangeEngine.Structures.StepResults


    type private PolicyRecord = OBWholeOfLife.PolicyRecord
    type private Gender = OBWholeOfLife.Gender
    type private LivesBasis = OBWholeOfLife.LivesBasis


    [<RequireQualifiedAccess; NoEquality; NoComparison>]
    type ExcelGender =
        | MALE
        | FEMALE

    [<RequireQualifiedAccess; NoEquality; NoComparison>]
    type ExcelLivesBasis =
        | SINGLE
        | JOINT

    [<NoEquality; NoComparison>]
    type OutputAttributes =
        {
            CashSurrenderBenefit            : float32
            DeathBenefit                    : float32
            UnsmoothedAssetShare            : float32
            SmoothedAssetShare              : float32
            ExitBonusRate                   : float32
            UnpaidPremiums                  : float32
            GuaranteedDeathBenefit          : float32
            DeathUpliftFactor               : float32

            Step0_Opening_UAS               : float32
            Step0_Opening_SAS               : float32
            Step1_RestatedAdjustments_UAS   : float32
            Step1_RestatedAdjustments_SAS   : float32
            Step2_RestatedActuals_UAS       : float32
            Step2_RestatedActuals_SAS       : float32
            Step3_RestatedDeductions_UAS    : float32
            Step3_RestatedDeductions_SAS    : float32
            Step4_MoveToClosingDate_UAS     : float32
            Step4_MoveToClosingDate_SAS     : float32
            Step5_Adjustments_UAS           : float32
            Step5_Adjustments_SAS           : float32
            Step6_Premiums_UAS              : float32
            Step6_Premiums_SAS              : float32
            Step7_Deductions_UAS            : float32
            Step7_Deductions_SAS            : float32
            Step8_MortalityCharge_UAS       : float32
            Step8_MortalityCharge_SAS       : float32
            Step9_InvestmentReturn_UAS      : float32
            Step9_InvestmentReturn_SAS      : float32
        }


    let getPolicyRelatedAttributes (OBWholeOfLife.PolicyRecord r) : PolicyRelatedAttributes =
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
                        AsyncResult.returnError "failed"
            }


    let createPostOpeningDispatcher<'TPolicyRecord>
        (config: PostOpeningDispatcherConfig)
        : WrappedApiRequestor<'TPolicyRecord, OutputAttributes> =
            WrappedApiRequestor {
                new IApiRequestor<_> with
                    member _.Name =
                        "PX API [Post-Opening Regression]"
                    member _.Execute field record =
                        AsyncResult.returnError "failed"
            }
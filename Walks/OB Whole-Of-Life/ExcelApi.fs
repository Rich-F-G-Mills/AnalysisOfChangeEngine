
namespace AnalysisOfChangeEngine.Walks.OBWholeOfLife


[<AutoOpen>]
module ExcelApi =

    open System
    open FsToolkit.ErrorHandling

    open AnalysisOfChangeEngine
    open AnalysisOfChangeEngine.Common
    open AnalysisOfChangeEngine.ApiProvider.Excel
    open AnalysisOfChangeEngine.Structures.PolicyRecords


    [<RequireQualifiedAccess>]
    /// Not intended for general use.
    // We can't make these hidden as the types must be available outside of this assembly.
    // However, we can 'bury' them in a module that no-one should need to access.
    module TransferTypes =

        [<NoEquality; NoComparison>]
        type ExcelPremiumFrequency =
            | MONTHLY
            | YEARLY

        [<RequireQualifiedAccess>]
        module ExcelPremiumFrequency =
        
            let internal ofUnderlying = function
                | OBWholeOfLife.PremiumFrequency.Monthly ->
                    ExcelPremiumFrequency.MONTHLY
                | OBWholeOfLife.PremiumFrequency.Yearly ->
                    ExcelPremiumFrequency.YEARLY


        [<NoEquality; NoComparison>]
        type ExcelLivesBasis =
            | SINGLE
            | JOINT

        [<RequireQualifiedAccess>]
        module ExcelLivesBasis =

            let internal ofUnderlying = function
                | OBWholeOfLife.LivesBasis.SingleLife _ ->
                    ExcelLivesBasis.SINGLE
                | OBWholeOfLife.LivesBasis.JointLife _ ->
                    ExcelLivesBasis.JOINT


        [<NoEquality; NoComparison>]
        type ExcelGender =
            | MALE
            | FEMALE

        [<RequireQualifiedAccess>]
        module ExcelGender =

            let internal ofUnderlying = function
                | OBWholeOfLife.Gender.Male ->
                    ExcelGender.MALE
                | OBWholeOfLife.Gender.Female ->
                    ExcelGender.FEMALE


        [<NoEquality; NoComparison>]
        type ExcelPolicyStatus =
            | PP
            | PUP
            | AP

        [<RequireQualifiedAccess>]
        module ExcelPolicyStatus =

            let internal ofUnderlying = function
                | OBWholeOfLife.PolicyStatus.PremiumPaying ->
                    ExcelPolicyStatus.PP
                | OBWholeOfLife.PolicyStatus.PaidUp ->
                    ExcelPolicyStatus.PUP
                | OBWholeOfLife.PolicyStatus.AllPaid ->
                    ExcelPolicyStatus.AP


        [<NoEquality; NoComparison>]
        type ExcelStepRelatedInputs =
            {
                [<ExcelRangeAlias("INPUT_OPENING_CALC_DATE")>]
                OpeningCalculationDate          : DateTime option

                [<ExcelRangeAlias("INPUT_CALC_DATE")>]
                ClosingCalculationDate          : DateTime        
            }

        [<NoEquality; NoComparison>]
        type ExcelPolicyRelatedInputs =
            {
                [<ExcelRangeAlias("INPUT_IS_TAXABLE?")>]
                IsTaxable                       : bool

                [<ExcelRangeAlias("INPUT_ENTRY_DATE")>]
                EntryDate                       : DateTime

                [<ExcelRangeAlias("INPUT_NPDD")>]
                NextPremiumDueDate              : DateTime
        
                [<ExcelRangeAlias("INPUT_PREM_FREQ")>]
                PremiumFrequency                : ExcelPremiumFrequency
            
                [<ExcelRangeAlias("INPUT_MODAL_PREM")>]
                ModalPremium                    : float32

                [<ExcelRangeAlias("INPUT_LTD_PAYMENT_TERM")>]
                LimitedPaymentPremium           : int

                [<ExcelRangeAlias("INPUT_SUM_ASSURED")>]
                SumAssured                      : float32

                [<ExcelRangeAlias("INPUT_POLICY_STATUS")>]
                PolicyStatus                    : ExcelPolicyStatus

                [<ExcelRangeAlias("INPUT_LIVES_BASIS")>]
                LivesBasis                      : ExcelLivesBasis

                [<ExcelRangeAlias("INPUT_ENTRY_AGE_1")>]
                EntryAge1                       : int

                [<ExcelRangeAlias("INPUT_GENDER_1")>]
                Gender1                         : ExcelGender

                [<ExcelRangeAlias("INPUT_ENTRY_AGE_2")>]
                EntryAge2                       : int option

                [<ExcelRangeAlias("INPUT_GENDER_2")>]
                Gender2                         : ExcelGender option

                [<ExcelRangeAlias("INPUT_JVA")>]
                JointValuationAge               : int option
            }

        [<RequireQualifiedAccess>]
        module ExcelPolicyRelatedInputs =
        
            let internal ofUnderlying (OBWholeOfLife.PolicyRecord polRecord)
                : ExcelPolicyRelatedInputs =
                    {
                        IsTaxable               = polRecord.Taxable
                        EntryDate               = polRecord.EntryDate.ToDateTimeMidnight ()
                        NextPremiumDueDate      = polRecord.NextPremiumDueDate.ToDateTimeMidnight ()
                        PremiumFrequency        =
                            ExcelPremiumFrequency.ofUnderlying polRecord.PremiumFrequency
                        ModalPremium            = polRecord.ModalPremium
                        LimitedPaymentPremium   = polRecord.LimitedPaymentTerm
                        SumAssured              = polRecord.SumAssured
                        PolicyStatus            =
                            ExcelPolicyStatus.ofUnderlying polRecord.Status
                        LivesBasis              =
                            ExcelLivesBasis.ofUnderlying polRecord.Lives
                        EntryAge1               = polRecord.Lives.EntryAgeLife1
                        Gender1                 =
                            ExcelGender.ofUnderlying polRecord.Lives.GenderLife1
                        EntryAge2               = polRecord.Lives.EntryAgeLife2
                        Gender2                 =
                            polRecord.Lives.GenderLife2 |> Option.map ExcelGender.ofUnderlying
                        JointValuationAge       = polRecord.Lives.JointValuationAge
                    }


    [<NoEquality; NoComparison>]
    // This sits outside of the module above as this DOES need to be
    // actively used outside by the walk logic.
    type ExcelOutputs =
        {
            [<ExcelRangeAlias("OUTPUT_CASH_SURRENDER_BENEFIT")>]
            CashSurrenderBenefit            : float32

            [<ExcelRangeAlias("OUTPUT_DEATH_BENEFIT")>]
            DeathBenefit                    : float32

            [<ExcelRangeAlias("OUTPUT_UNSMOOTHED_ASSET_SHARE")>]
            UnsmoothedAssetShare            : float32

            [<ExcelRangeAlias("OUTPUT_SMOOTHED_ASSET_SHARE")>]
            SmoothedAssetShare              : float32

            [<ExcelRangeAlias("OUTPUT_EXIT_BONUS_RATE")>]
            ExitBonusRate                   : float32

            [<ExcelRangeAlias("OUTPUT_UNPAID_PREMIUMS")>]
            UnpaidPremiums                  : float32

            [<ExcelRangeAlias("OUTPUT_GUARANTEED_DEATH_BENEFIT")>]
            GuaranteedDeathBenefit          : float32

            [<ExcelRangeAlias("OUTPUT_DEATH_UPLIFT_FACTOR")>]
            DeathUpliftFactor               : float32

            [<ExcelRangeAlias("OUTPUT_STEP0_OPENING_UAS")>]
            Step0_Opening_UAS               : float32

            [<ExcelRangeAlias("OUTPUT_STEP0_OPENING_SAS")>]
            Step0_Opening_SAS               : float32

            [<ExcelRangeAlias("OUTPUT_STEP1_RESTATED_ADJUSTMENTS_UAS")>]
            Step1_RestatedAdjustments_UAS   : float32

            [<ExcelRangeAlias("OUTPUT_STEP1_RESTATED_ADJUSTMENTS_SAS")>]
            Step1_RestatedAdjustments_SAS   : float32

            [<ExcelRangeAlias("OUTPUT_STEP2_RESTATED_ACTUALS_UAS")>]
            Step2_RestatedActuals_UAS       : float32

            [<ExcelRangeAlias("OUTPUT_STEP2_RESTATED_ACTUALS_SAS")>]
            Step2_RestatedActuals_SAS       : float32

            [<ExcelRangeAlias("OUTPUT_STEP3_RESTATED_DEDUCTIONS_UAS")>]
            Step3_RestatedDeductions_UAS    : float32

            [<ExcelRangeAlias("OUTPUT_STEP3_RESTATED_DEDUCTIONS_SAS")>]
            Step3_RestatedDeductions_SAS    : float32

            [<ExcelRangeAlias("OUTPUT_STEP4_MOVE_TO_CLOSING_DATE_UAS")>]
            Step4_MoveToClosingDate_UAS     : float32

            [<ExcelRangeAlias("OUTPUT_STEP4_MOVE_TO_CLOSING_DATE_SAS")>]
            Step4_MoveToClosingDate_SAS     : float32

            [<ExcelRangeAlias("OUTPUT_STEP5_ADJUSTMENTS_UAS")>]
            Step5_Adjustments_UAS           : float32

            [<ExcelRangeAlias("OUTPUT_STEP5_ADJUSTMENTS_SAS")>]
            Step5_Adjustments_SAS           : float32

            [<ExcelRangeAlias("OUTPUT_STEP6_PREMIUMS_UAS")>]
            Step6_Premiums_UAS              : float32

            [<ExcelRangeAlias("OUTPUT_STEP6_PREMIUMS_SAS")>]
            Step6_Premiums_SAS              : float32

            [<ExcelRangeAlias("OUTPUT_STEP7_DEDUCTIONS_UAS")>]
            Step7_Deductions_UAS            : float32

            [<ExcelRangeAlias("OUTPUT_STEP7_DEDUCTIONS_SAS")>]
            Step7_Deductions_SAS            : float32

            [<ExcelRangeAlias("OUTPUT_STEP8_MORTALITY_CHARGE_UAS")>]
            Step8_MortalityCharge_UAS       : float32

            [<ExcelRangeAlias("OUTPUT_STEP8_MORTALITY_CHARGE_SAS")>]
            Step8_MortalityCharge_SAS       : float32

            [<ExcelRangeAlias("OUTPUT_STEP9_INVESTMENT_RETURN_UAS")>]
            Step9_InvestmentReturn_UAS      : float32

            [<ExcelRangeAlias("OUTPUT_STEP9_INVESTMENT_RETURN_SAS")>]
            Step9_InvestmentReturn_SAS      : float32
        }

    let createExcelDispatcher workbookSelector cancellationToken =
        ApiProvider.Excel.Dispatcher.createExcelDispatcher<TransferTypes.ExcelStepRelatedInputs, TransferTypes.ExcelPolicyRelatedInputs>
            workbookSelector cancellationToken


    type IWrappedExcelApiRequestors =
        interface
            inherit IDisposable

            abstract member Opening     : WrappedApiRequestor<OBWholeOfLife.PolicyRecord, ExcelOutputs>
            abstract member PostOpening : WrappedApiRequestor<OBWholeOfLife.PolicyRecord, ExcelOutputs>
        end


    let createOpeningExcelRequestor
        (dispatcher: IExcelDispatcher<_, _>) (openingRunDate: DateOnly option)
        : WrappedApiRequestor<OBWholeOfLife.PolicyRecord, ExcelOutputs> =
            let requestorName =
                "Excel API [Opening]"

            let apiRequestor =
                match openingRunDate with
                | Some openingRunDate' ->
                    let stepRelatedInputs: TransferTypes.ExcelStepRelatedInputs =
                        {
                            OpeningCalculationDate = None
                            ClosingCalculationDate = openingRunDate'.ToDateTimeMidnight ()
                        }

                    let executor requiredOutputs policyRecord =
                        backgroundTask {
                            let policyRelatedInputs =
                                TransferTypes.ExcelPolicyRelatedInputs.ofUnderlying policyRecord

                            let! requestOutcome, dispatcherTelemetry =
                                dispatcher.ExecuteAsync requiredOutputs (stepRelatedInputs, policyRelatedInputs)

                            let apiTelemetry: ApiRequestTelemetry option =
                                dispatcherTelemetry
                                |> Option.map (fun telemetry ->
                                    {
                                        EndpointId      = Some $"{telemetry.EndpointIdx}"
                                        ProcessingStart = telemetry.ProcessingStart
                                        ProcessingEnd   = telemetry.ProcessingEnd                                   
                                    })

                            return requestOutcome, apiTelemetry                        
                        }                                         

                    ApiRequestor.create (requestorName, executor)

                | None ->
                    let executor _ _ =
                        failwith "Cannot call the opening requestor when no prior run date is provided."                

                    ApiRequestor.create (requestorName, executor)

            WrappedApiRequestor apiRequestor


    let createPostOpeningExcelRequestor
        (dispatcher: IExcelDispatcher<_,_>) (openingRunDate: DateOnly option, closingRunDate: DateOnly)
        : WrappedApiRequestor<OBWholeOfLife.PolicyRecord, ExcelOutputs> =
            let stepRelatedInputs: TransferTypes.ExcelStepRelatedInputs =
                {
                    OpeningCalculationDate =
                        openingRunDate |> Option.map _.ToDateTimeMidnight()
                    ClosingCalculationDate =
                        closingRunDate.ToDateTimeMidnight ()
                }

            let executor requiredOutputs policyRecord =
                backgroundTask {
                    let policyRelatedInputs =
                        TransferTypes.ExcelPolicyRelatedInputs.ofUnderlying policyRecord

                    let! requestOutcome, dispatcherTelemetry =
                        dispatcher.ExecuteAsync requiredOutputs (stepRelatedInputs, policyRelatedInputs)

                    let apiTelemetry: ApiRequestTelemetry option =
                        dispatcherTelemetry
                        |> Option.map (fun telemetry ->
                            {
                                EndpointId      = Some $"{telemetry.EndpointIdx}"
                                ProcessingStart = telemetry.ProcessingStart
                                ProcessingEnd   = telemetry.ProcessingEnd                                   
                            })

                    return requestOutcome, apiTelemetry                        
                } 

            let apiRequestor =
                ApiRequestor.create ("PX API [Post-Opening Regression]", executor)

            WrappedApiRequestor apiRequestor

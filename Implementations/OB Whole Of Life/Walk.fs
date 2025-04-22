
namespace AnalysisOfChangeEngine.Implementations.OBWholeOfLife

open System
open FSharp.Quotations
open FsToolkit.ErrorHandling
open AnalysisOfChangeEngine
open AnalysisOfChangeEngine.Implementations


// Retain the compiler generated structural equality as used later.
[<NoComparison>]
type StepResults =
    {
        UnsmoothedAssetShare    : double
        SmoothedAssetShare      : double
        GuaranteedDeathBenefit  : double
        SurrenderValue          : double
        DeathBenefit            : double
        ExitBonusRate           : double
        UnpaidPremiums          : double
        DeathUpliftFactor       : double
    }


[<NoEquality; NoComparison>]
type WalkConfiguration =
    {        
        StepFactory: StepFactory
        PxDispatcher: Object
    }


[<NoEquality; NoComparison>]
type ApiCollection =
    {
        px_OpeningRegression        : WrappedApiRequestor<PolicyRecord, PxApi.OutputAttributes>
        px_PostOpeningRegression    : WrappedApiRequestor<PolicyRecord, PxApi.OutputAttributes>
    }


[<Sealed>]
type Walk private (logger: ILogger, runContext: RunContext, config: WalkConfiguration) as this =
    inherit AbstractWalk<PolicyRecord, StepResults, ApiCollection> (logger)


    // --- HELPERS ---

    let createStep =
        config.StepFactory

    // Short-hand way of changing our source for UAS and SAS (post-opening regression!)
    let useForAssetShares
        (unsmoothedSelector: Expr<PxApi.OutputAttributes -> double>)
        (smoothedSelector: Expr<PxApi.OutputAttributes -> double>)
        : SourceExpr<PolicyRecord, StepResults, _> =
            <@
                fun from _ prior _ ->
                    {
                        prior with
                            UnsmoothedAssetShare =
                                from.apiCall (_.px_PostOpeningRegression, %unsmoothedSelector)
                            SmoothedAssetShare =
                                from.apiCall (_.px_PostOpeningRegression, %smoothedSelector)
                    }
            @>


    // --- DATA CHANGERS ---

    let dataChanger_RestatedOpening
        : PolicyRecordChanger<PolicyRecord> =
            fun (opening, _, closing) ->
                let restatedStatus =
                    match opening.Status, closing.Status with
                    | PolicyStatus.PremiumPaying, PolicyStatus.PaidUp ->
                        // Don't transition to PUP at this time.
                        PolicyStatus.PremiumPaying
                    | _ ->
                        // It doesn't matter whether we pick that from the opening or closing!
                        closing.Status

                let restated =
                    {
                        opening with
                            TableCode = closing.TableCode
                            EntryDate = closing.EntryDate
                            SumAssured = closing.SumAssured
                            Lives = closing.Lives
                            LimitedPaymentTerm = closing.LimitedPaymentTerm
                            Status = restatedStatus
                            // We intentionally don't change the NPDD at this time.
                    }

                // If nothing has changed, return None as this will indicate to the
                // calling logic that nothing has changed for this record.
                if opening <> restated then Some restated else None
            
    let dataChanger_RollForward
        : PolicyRecordChanger<PolicyRecord> =
            fun (_, prior, closing) ->
                let updatedStatus, updatedNPDD =
                    match prior.Status, closing.Status with
                    | PolicyStatus.PremiumPaying, PolicyStatus.PaidUp ->
                        let rollForwardNPDD =
                            // Ensure that we at least roll it forward to the closing date.
                            DateOnly.Max (prior.NextPremiumDueDate, runContext.ClosingRunDate)

                        // We roll-forward the NPDD assuming the premium had been paid.
                        PolicyStatus.PremiumPaying, rollForwardNPDD

                    | _ ->
                        // It doesn't matter whether we pick that from the opening or closing!
                        closing.Status, closing.NextPremiumDueDate                       

                let updated =
                    {
                        prior with
                            Status = updatedStatus
                            NextPremiumDueDate = updatedNPDD
                    }

                if prior <> updated then Some updated else None
            
    let dataChanger_RecentPaidUps
        : PolicyRecordChanger<PolicyRecord> =
            fun (_, prior, closing) ->
                let updatedStatus, updatedNPDD =
                    match prior.Status, closing.Status with
                    | PolicyStatus.PremiumPaying, PolicyStatus.PaidUp ->
                        PolicyStatus.PaidUp, closing.NextPremiumDueDate
                    | _ ->
                        prior.Status, prior.NextPremiumDueDate

                let updated =
                    {
                        prior with
                            Status = updatedStatus
                            NextPremiumDueDate = updatedNPDD
                    }

                if prior <> updated then Some updated else None


    // --- CONTROL CREATION ---

    static member create (logger, runContext, config) =
        result {
            return new Walk (logger, runContext, config)
        }


    // --- API COLLECTION ---

    // DD - We've chosen to define the API collection itself within the walk.
    // This was just so that we can keep everything together.
    member val ApiCollection =
        {
            px_OpeningRegression =
                PxApi.createOpeningDispatcher<PolicyRecord> {
                    OpeningRunDate =
                        runContext.OpeningRunDate
                }
            px_PostOpeningRegression =
                PxApi.createPostOpeningDispatcher<PolicyRecord> {
                    OpeningRunDate =
                        runContext.OpeningRunDate
                    ClosingRunDate =
                        runContext.ClosingRunDate
                }
        }
             

    // --- REQUIRED STEPS ---

    override val Opening =
        createStep.opening  {
            Validator = noValidator
        }
         
    override val OpeningReRun =
        createStep.openingReRun {
            Source = <@
                fun from _ _ ->
                    {                    
                        UnsmoothedAssetShare =
                            from.apiCall (_.px_OpeningRegression, _.UnsmoothedAssetShare)
                        SmoothedAssetShare =
                            from.apiCall (_.px_OpeningRegression, _.SmoothedAssetShare)
                        GuaranteedDeathBenefit =
                            from.apiCall (_.px_OpeningRegression, _.GuaranteedDeathBenefit)
                        ExitBonusRate =
                            from.apiCall (_.px_OpeningRegression, _.ExitBonusRate)
                        UnpaidPremiums =
                            from.apiCall (_.px_OpeningRegression, _.UnpaidPremiums)
                        SurrenderValue =
                            from.apiCall (_.px_OpeningRegression, _.CashSurrenderValue)
                        DeathUpliftFactor =
                            from.apiCall (_.px_OpeningRegression, _.DeathUpliftFactor)
                        DeathBenefit =
                            from.apiCall (_.px_OpeningRegression, _.DeathBenefit)
                    } @>

            Validator =
                fun (_, beforeResults, afterResults) ->
                    if beforeResults = afterResults then
                        List.empty
                    else
                        [ StepValidationIssue.Warning "Regression mis-match" ]
        }    

    override val RemoveExitedRecords =
        createStep.removeExited ()


    // --- PRODUCT SPECIFIC STEPS ---
    // We need to make sure that these are registered as well as defined!

    // We can only restate data which is still in-force!
    member val RestatedOpeningData =
        this.registerInteriorStep(
            createStep.restatedOpeningData {
                DataChanger =
                    dataChanger_RestatedOpening
                Validator =
                    noValidator            
            }
        )

    member val AocOpeningConsistencyCheck =
        // The interested reader may be wondering why this is needed...
        // As of the previous step, we're using the same UAS and SAS outputs as
        // underlying claims calculations. From this step onwards, we're using
        // UAS and SAS outputs as provided by PX's AoC machinery. In theory,
        // they should (!) always tie up. This just proves it.
        this.registerInteriorStep(        
            createStep.aocOpeningConsistencyCheck {
                Source = <@
                    fun from _ prior current ->
                        {
                            prior with
                                UnsmoothedAssetShare =
                                    from.apiCall (_.px_PostOpeningRegression, _.Step0_Opening_UAS)
                                SmoothedAssetShare =
                                    from.apiCall (_.px_PostOpeningRegression, _.Step0_Opening_SAS)
                                SurrenderValue =
                                    current.SmoothedAssetShare * (1.0 + current.ExitBonusRate)
                                        - current.UnpaidPremiums
                                DeathBenefit =
                                    Math.Max(
                                        current.SmoothedAssetShare * current.DeathUpliftFactor * (1.0 + current.ExitBonusRate),
                                        current.GuaranteedDeathBenefit
                                    ) - current.UnpaidPremiums
                        } @>

                Validator =
                    noValidator
            }
        )          

    member val RestatedOpeningAdjustments =
        this.registerInteriorStep(
            createStep.restatedOpeningAdjustments {
                Source =
                    useForAssetShares
                        <@ _.Step1_RestatedAdjustments_UAS @>
                        <@ _.Step1_RestatedAdjustments_SAS @>

                Validator =
                    noValidator
            }
        )

    member val RestatedOpeningReturns =
        this.registerInteriorStep(
            createStep.restatedOpeningReturns {
                Source =
                    useForAssetShares
                        <@ _.Step2_RestatedActuals_UAS @>
                        <@ _.Step2_RestatedActuals_SAS @>

                Validator =
                    noValidator
            }
        )
            
    member val RestatedOpeningDeductions =
        this.registerInteriorStep(
            createStep.restatedOpeningDeductions {
                Source =
                    useForAssetShares
                        <@ _.Step3_RestatedDeductions_UAS @>
                        <@ _.Step3_RestatedDeductions_SAS @>
                    
                Validator =
                    noValidator
            }
        )   
        
    member val MoveToClosingDate =
        this.registerInteriorStep(
            createStep.moveToClosingDate {
                Source =
                    useForAssetShares
                        <@ _.Step4_MoveToClosingDate_UAS @>
                        <@ _.Step4_MoveToClosingDate_SAS @>

                Validator =
                    noValidator
            }
        )

    member val DataRollForward =
        this.registerInteriorStep (
            createStep.dataRollForward {
                DataChanger =
                    dataChanger_RollForward
                Validator =
                    noValidator
            }
        )

    member val Adjustments =
        this.registerInteriorStep (
            createStep.adjustments {
                Source =
                    useForAssetShares
                        <@ _.Step5_Adjustments_UAS @>
                        <@ _.Step5_Adjustments_SAS @>

                Validator =
                    noValidator
            }
        )

    member val Premiums =
        this.registerInteriorStep (
            createStep.premiums {
                Source =
                    useForAssetShares
                        <@ _.Step6_Premiums_UAS @>
                        <@ _.Step6_Premiums_SAS @>

                Validator =
                    noValidator
            }
        )

    member val Deductions =
        this.registerInteriorStep (
            createStep.deductions {
                Source =
                    useForAssetShares
                        <@ _.Step7_Deductions_UAS @>
                        <@ _.Step7_Deductions_SAS @>

                Validator =
                    noValidator
            }
        )

    member val MortalityCharges =
        this.registerInteriorStep (
            createStep.mortalityCharges {
                Source =
                    useForAssetShares
                        <@ _.Step8_MortalityCharge_UAS @>
                        <@ _.Step8_MortalityCharge_SAS @>

                Validator =
                    noValidator
            }
        )

    member val InvestmentReturns =
        this.registerInteriorStep (
            createStep.investmentReturns {
                Source =
                    useForAssetShares
                        <@ _.Step9_InvestmentReturn_UAS @>
                        <@ _.Step9_InvestmentReturn_SAS @>

                Validator =
                    noValidator
            }
        )

    member val ClosingGuaranteedDeathBenefit =
        this.registerInteriorStep (
            createStep.closingGuaranteedDeathBenefit {
                Source = <@
                    fun from _ prior _ ->
                        {
                            prior with
                                GuaranteedDeathBenefit =
                                    from.apiCall (_.px_PostOpeningRegression, _.GuaranteedDeathBenefit)
                        } @>

                Validator =
                    noValidator
            }
        )

    member val ClosingDeathUpliftFactor =
        this.registerInteriorStep (
            createStep.closingDeathUpliftFactor {
                Source = <@
                    fun from _ prior _ ->
                        {
                            prior with
                                DeathUpliftFactor =
                                    from.apiCall (_.px_PostOpeningRegression, _.DeathUpliftFactor)
                        } @>

                Validator =
                    noValidator
            }
        )

    member val ClosingExitBonusRate =
        this.registerInteriorStep (
            createStep.closingExitBonusRate {
                Source = <@
                    fun from _ prior _ ->
                        {
                            prior with
                                ExitBonusRate =
                                    from.apiCall (_.px_PostOpeningRegression, _.ExitBonusRate)
                        } @>

                Validator =
                    noValidator
            }
        )

    // Same rationale as per the opening equivalent.
    member val AocClosingConsistencyCheck =
        this.registerInteriorStep (
            createStep.aocClosingConsistencyCheck {
                Source = <@
                    fun from _ _ _ ->
                        {                    
                            UnsmoothedAssetShare =
                                from.apiCall (_.px_PostOpeningRegression, _.UnsmoothedAssetShare)
                            SmoothedAssetShare =
                                from.apiCall (_.px_PostOpeningRegression, _.SmoothedAssetShare)
                            GuaranteedDeathBenefit =
                                from.apiCall (_.px_PostOpeningRegression, _.GuaranteedDeathBenefit)
                            ExitBonusRate =
                                from.apiCall (_.px_PostOpeningRegression, _.ExitBonusRate)
                            UnpaidPremiums =
                                from.apiCall (_.px_PostOpeningRegression, _.UnpaidPremiums)
                            SurrenderValue =
                                from.apiCall (_.px_PostOpeningRegression, _.CashSurrenderValue)
                            DeathUpliftFactor =
                                from.apiCall (_.px_PostOpeningRegression, _.DeathUpliftFactor)
                            DeathBenefit =
                                from.apiCall (_.px_PostOpeningRegression, _.DeathBenefit)
                        } @>

                Validator =
                    noValidator
            }
        )

    member val RecentPaidUps  =
        this.registerInteriorStep (
            createStep.recentPaidUps {
                DataChanger =
                    dataChanger_RecentPaidUps
                Validator =
                    noValidator
            }
        )

    // --- REQUIRED STEPS ---

    override val MoveToClosingData =
        createStep.moveToClosingData {            
            Validator =
                noValidator
        }
 
    override val AddNewRecords =
        createStep.addNewRecords {
            Validator = noValidator
        }

namespace AnalysisOfChangeEngine.Walks.OBWholeOfLife


open System
open System.Threading
open FSharp.Quotations
open FsToolkit.ErrorHandling
open AnalysisOfChangeEngine
open AnalysisOfChangeEngine.Common
open AnalysisOfChangeEngine.Structures.PolicyRecords
open AnalysisOfChangeEngine.Structures.StepResults
open AnalysisOfChangeEngine.Walks
open AnalysisOfChangeEngine.Walks.Common


[<NoEquality; NoComparison>]
type WalkConfiguration =
    {        
        StepFactory                 : StepFactory
        IgnoreOpeningMismatches     : bool
        OpeningRunDate              : DateOnly option
        ClosingRunDate              : DateOnly
    }

[<NoEquality; NoComparison>]
type ApiCollection =
    {
        xl_OpeningRegression        : WrappedApiRequestor<OBWholeOfLife.PolicyRecord, ExcelOutputs>
        xl_PostOpeningRegression    : WrappedApiRequestor<OBWholeOfLife.PolicyRecord, ExcelOutputs>
    }


[<Sealed>]
type Walk private (logger: ILogger, config: WalkConfiguration) as this =
    inherit AbstractWalk<OBWholeOfLife.PolicyRecord, OBWholeOfLife.StepResults, ApiCollection> (logger)


    // --- HELPERS ---

    let createStep =
        config.StepFactory

    // Short-hand way of changing our source for UAS and SAS (post-opening regression!)
    let useForAssetShares
        (unsmoothedSelector: Expr<ExcelOutputs -> float32>)
        (smoothedSelector: Expr<ExcelOutputs -> float32>)
        : SourceExpr<OBWholeOfLife.PolicyRecord, OBWholeOfLife.StepResults, _> =
            <@
                fun from _ prior _ ->
                    {
                        prior with
                            UnsmoothedAssetShare =
                                from.apiCall (_.xl_PostOpeningRegression, %unsmoothedSelector)
                            SmoothedAssetShare =
                                from.apiCall (_.xl_PostOpeningRegression, %smoothedSelector)
                    }
            @>


    // --- DATA CHANGERS ---

    let dataChanger_RestatedOpening
        : PolicyRecordChanger<_> =
            fun (OBWholeOfLife.PolicyRecord opening, _, OBWholeOfLife.PolicyRecord closing) ->
                result {
                    let restatedStatus =
                        match opening.Status, closing.Status with
                        | OBWholeOfLife.PolicyStatus.PremiumPaying, OBWholeOfLife.PolicyStatus.PaidUp ->
                            // Don't transition to PUP at this time.
                            OBWholeOfLife.PolicyStatus.PremiumPaying
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
                    if opening <> restated then
                        let! restated' =
                            OBWholeOfLife.PolicyRecord.validate restated

                        return Some restated'
                    else
                        return None
                }
                    
            
    let dataChanger_RollForward
        : PolicyRecordChanger<_> =
            fun (_, OBWholeOfLife.PolicyRecord prior, OBWholeOfLife.PolicyRecord closing) ->
                result {
                    let updatedStatus, updatedNPDD =
                        match prior.Status, closing.Status with
                        | OBWholeOfLife.PolicyStatus.PremiumPaying, OBWholeOfLife.PolicyStatus.PaidUp ->
                            let rollForwardNPDD =
                                // Ensure that we at least roll it forward to the closing date.
                                DateOnly.Max (prior.NextPremiumDueDate, config.ClosingRunDate)

                            // We roll-forward the NPDD assuming the premium had been paid.
                            OBWholeOfLife.PolicyStatus.PremiumPaying, rollForwardNPDD

                        | _ ->
                            // It doesn't matter whether we pick that from the opening or closing!
                            closing.Status, closing.NextPremiumDueDate                       

                    let updated =
                        {
                            prior with
                                Status = updatedStatus
                                NextPremiumDueDate = updatedNPDD
                        }                        

                    if prior <> updated then
                        let! updated' =
                            OBWholeOfLife.PolicyRecord.validate updated

                        return Some updated'
                    else
                        return None
                }
                    
            
    let dataChanger_RecentPaidUps
        : PolicyRecordChanger<_> =
            fun (_, OBWholeOfLife.PolicyRecord prior, OBWholeOfLife.PolicyRecord closing) ->
                result {
                    let updatedStatus, updatedNPDD =
                        match prior.Status, closing.Status with
                        | OBWholeOfLife.PolicyStatus.PremiumPaying, OBWholeOfLife.PolicyStatus.PaidUp ->
                            OBWholeOfLife.PolicyStatus.PaidUp, closing.NextPremiumDueDate
                        | _ ->
                            prior.Status, prior.NextPremiumDueDate

                    let updated =
                        {
                            prior with
                                Status = updatedStatus
                                NextPremiumDueDate = updatedNPDD
                        }                        

                    if prior <> updated then
                        let! updated' =
                            OBWholeOfLife.PolicyRecord.validate updated

                        return Some updated'
                    else
                        return None
                }
                  
               
    // --- API COLLECTION ---

    (*
    Design Decision:
        We've chosen to define the API collection itself within the walk.
        This was just so that we can keep everything together.
    *)

    let excelDispatcher =
        createExcelDispatcher
            _.StartsWith("as_calc") CancellationToken.None

    // We want this to be publicly available.
    member val ApiCollection =
        {
            xl_OpeningRegression =
                ExcelApi.createOpeningExcelRequestor
                    excelDispatcher config.OpeningRunDate

            xl_PostOpeningRegression =
                ExcelApi.createPostOpeningExcelRequestor
                    excelDispatcher (config.OpeningRunDate, config.ClosingRunDate)
        }


    // --- CONTROL CREATION ---

    static member create (logger, config) =
        result {
            return new Walk (logger, config)
        }
             

    // --- REQUIRED STEPS ---
         
    override val OpeningReRun =
        createStep.openingReRun {
            Source = <@
                fun from _ _ ->
                    {                    
                        UnsmoothedAssetShare =
                            from.apiCall (_.xl_OpeningRegression, _.UnsmoothedAssetShare)
                        SmoothedAssetShare =
                            from.apiCall (_.xl_OpeningRegression, _.SmoothedAssetShare)
                        GuaranteedDeathBenefit =
                            from.apiCall (_.xl_OpeningRegression, _.GuaranteedDeathBenefit)
                        ExitBonusRate =
                            from.apiCall (_.xl_OpeningRegression, _.ExitBonusRate)
                        UnpaidPremiums =
                            from.apiCall (_.xl_OpeningRegression, _.UnpaidPremiums)
                        SurrenderBenefit =
                            from.apiCall (_.xl_OpeningRegression, _.CashSurrenderBenefit)
                        DeathUpliftFactor =
                            from.apiCall (_.xl_OpeningRegression, _.DeathUpliftFactor)
                        DeathBenefit =
                            from.apiCall (_.xl_OpeningRegression, _.DeathBenefit)
                    } : OBWholeOfLife.StepResults @>

            Validator = function
                | (_, Some _, _) when config.IgnoreOpeningMismatches ->
                    StepValidationOutcome.Empty

                | (_, Some beforeResults, afterResults) when beforeResults.IsCloseTo afterResults ->                        
                    StepValidationOutcome.Empty

                | (_, Some _, _) ->                            
                    StepValidationOutcome.Completed [| "Regression mis-match." |]

                | (_, None, _) when config.IgnoreOpeningMismatches ->
                    StepValidationOutcome.Empty

                | (_, None, _) ->
                    StepValidationOutcome.Aborted "No opening results for comparison."
        }    

    override val RemoveExitedRecords =
        createStep.removeExited ()


    // --- PRODUCT SPECIFIC STEPS ---
    // We need to make sure that these are registered in order to be discoverable.

    // We can only restate data which is still in-force!
    member val RestatedOpeningData =
        this.registerInteriorStep(
            createStep.restatedOpeningData {
                DataChanger =
                    dataChanger_RestatedOpening
                Validator =
                    // No doubt we could do something convoluted here. However, given such
                    // changes aren't (regularly) expected, would have minimal benefit.
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
                                    from.apiCall (_.xl_PostOpeningRegression, _.Step0_Opening_UAS)
                                SmoothedAssetShare =
                                    from.apiCall (_.xl_PostOpeningRegression, _.Step0_Opening_SAS)
                                SurrenderBenefit =
                                    current.SmoothedAssetShare * (1.0f + current.ExitBonusRate)
                                        - current.UnpaidPremiums
                                DeathBenefit =
                                    Math.Max(
                                        current.SmoothedAssetShare * current.DeathUpliftFactor * (1.0f + current.ExitBonusRate),
                                        current.GuaranteedDeathBenefit
                                    ) - current.UnpaidPremiums
                        } : OBWholeOfLife.StepResults @>

                Validator = function
                    // TODO - May need to add some kind of tolerance here.
                    | (_, beforeResults, afterResults) when beforeResults.IsCloseTo afterResults ->                        
                        StepValidationOutcome.Empty

                    | _ ->
                    //| (_, beforeResults, afterResults) ->
                        //do printfn "\n\n-----------------\n%A\n-----------------\n%A\n\n" beforeResults afterResults

                        StepValidationOutcome.Completed [| "Mismatch between opening position in AoC logic." |]
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
                                    from.apiCall (_.xl_PostOpeningRegression, _.GuaranteedDeathBenefit)
                        } : OBWholeOfLife.StepResults @>

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
                                    from.apiCall (_.xl_PostOpeningRegression, _.DeathUpliftFactor)
                        } : OBWholeOfLife.StepResults @>

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
                                    from.apiCall (_.xl_PostOpeningRegression, _.ExitBonusRate)
                        } : OBWholeOfLife.StepResults @>

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
                                from.apiCall (_.xl_PostOpeningRegression, _.UnsmoothedAssetShare)
                            SmoothedAssetShare =
                                from.apiCall (_.xl_PostOpeningRegression, _.SmoothedAssetShare)
                            GuaranteedDeathBenefit =
                                from.apiCall (_.xl_PostOpeningRegression, _.GuaranteedDeathBenefit)
                            ExitBonusRate =
                                from.apiCall (_.xl_PostOpeningRegression, _.ExitBonusRate)
                            UnpaidPremiums =
                                from.apiCall (_.xl_PostOpeningRegression, _.UnpaidPremiums)
                            SurrenderBenefit =
                                from.apiCall (_.xl_PostOpeningRegression, _.CashSurrenderBenefit)
                            DeathUpliftFactor =
                                from.apiCall (_.xl_PostOpeningRegression, _.DeathUpliftFactor)
                            DeathBenefit =
                                from.apiCall (_.xl_PostOpeningRegression, _.DeathBenefit)
                        } : OBWholeOfLife.StepResults @>

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
            Validator =
                noValidator
        }
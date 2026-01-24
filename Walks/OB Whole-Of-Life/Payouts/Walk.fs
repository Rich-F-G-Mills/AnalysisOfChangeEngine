
namespace AnalysisOfChangeEngine.Walks.OBWholeOfLife.Payouts


open System
open System.Threading
open FsToolkit.ErrorHandling
open AnalysisOfChangeEngine
open AnalysisOfChangeEngine.Common
open AnalysisOfChangeEngine.Structures.PolicyRecords
open AnalysisOfChangeEngine.Structures.StepResults
open AnalysisOfChangeEngine.Walks
open AnalysisOfChangeEngine.Walks.Common
open AnalysisOfChangeEngine.Walks.OBWholeOfLife


[<NoEquality; NoComparison>]
type ApiCollection =
    {
        xl_OpeningRegression        : WrappedApiRequestor<OBWholeOfLife.PolicyRecord, ExcelOutputs>
        xl_PostOpeningRegression    : WrappedApiRequestor<OBWholeOfLife.PolicyRecord, ExcelOutputs>
        xl_ClosingMonthEnd          : WrappedApiRequestor<OBWholeOfLife.PolicyRecord, ExcelOutputs>
    }


[<NoEquality; NoComparison>]
type WalkConfiguration =
    {        
        StepFactory                 : StepFactory<OBWholeOfLife.PolicyRecord, OBWholeOfLife.Payouts.StepResults, ApiCollection>
        IgnoreOpeningMismatches     : bool
        OpeningRunDate              : DateOnly option
        ClosingRunDate              : DateOnly
    }


[<Sealed>]
type Walk private (logger: ILogger, config: WalkConfiguration) as this =
    inherit AbstractWalk<OBWholeOfLife.PolicyRecord, OBWholeOfLife.Payouts.StepResults, ApiCollection> (logger)


    // --- HELPERS ---

    let createStep =
        config.StepFactory
                  
               
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

            xl_ClosingMonthEnd =
                ExcelApi.createClosingMonthEndExcelRequestor
                    excelDispatcher config.ClosingRunDate
        }


    // --- CONTROL CREATION ---

    static member create logger config =
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
                    } @>

            Validator = function
                | (_, None, _) ->
                    // If the results aren't available... They aren't available!
                    // No sensible reason to raise an issue/abort just for this.
                    StepValidationOutcome.Completed

                | (_, Some _, _) when config.IgnoreOpeningMismatches ->
                    StepValidationOutcome.Completed

                | (_, Some beforeResults, afterResults) when beforeResults.IsCloseTo afterResults ->                        
                    StepValidationOutcome.Completed

                | (_, Some _, _) ->                            
                    StepValidationOutcome.CompletedWithIssues
                        (nonEmptyList { yield "Regression mis-match." })
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
                    StepValidationOutcome.noValidator            
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
                        } @>

                Validator = function
                    // TODO - May need to add some kind of tolerance here.
                    | (_, beforeResults, afterResults) when beforeResults.IsCloseTo afterResults ->                        
                        StepValidationOutcome.Completed

                    | _ ->
                    //| (_, beforeResults, afterResults) ->
                        //do printfn "\n\n-----------------\n%A\n-----------------\n%A\n\n" beforeResults afterResults

                        StepValidationOutcome.CompletedWithIssues 
                            (nonEmptyList { yield  "Mismatch between opening position in AoC logic." })
            }
        )          

    member val RestatedOpeningAdjustments =
        this.registerInteriorStep(
            createStep.restatedOpeningAdjustments {
                Source = <@
                    fun from _ prior _ ->
                        {
                            prior with
                                UnsmoothedAssetShare =
                                    from.apiCall (_.xl_PostOpeningRegression, _.Step1_RestatedAdjustments_UAS)
                                SmoothedAssetShare =
                                    from.apiCall (_.xl_PostOpeningRegression, _.Step1_RestatedAdjustments_SAS)
                        } @>

                Validator =
                    StepValidationOutcome.noValidator
            }
        )

    member val RestatedOpeningReturns =
        this.registerInteriorStep(
            createStep.restatedOpeningReturns {
                Source = <@
                    fun from _ prior _ ->
                        {
                            prior with
                                UnsmoothedAssetShare =
                                    from.apiCall (_.xl_PostOpeningRegression, _.Step2_RestatedActuals_UAS)
                                SmoothedAssetShare =
                                    from.apiCall (_.xl_PostOpeningRegression, _.Step2_RestatedActuals_SAS)
                        } @>

                Validator =
                    StepValidationOutcome.noValidator
            }
        )
            
    member val RestatedOpeningDeductions =
        this.registerInteriorStep(
            createStep.restatedOpeningDeductions {
                Source = <@
                    fun from _ prior _ ->
                        {
                            prior with
                                UnsmoothedAssetShare =
                                    from.apiCall (_.xl_PostOpeningRegression, _.Step3_RestatedDeductions_UAS)
                                SmoothedAssetShare =
                                    from.apiCall (_.xl_PostOpeningRegression, _.Step3_RestatedDeductions_SAS)
                        } @>
                    
                Validator =
                    StepValidationOutcome.noValidator
            }
        )   
        
    member val MoveToClosingDate =
        this.registerInteriorStep(
            createStep.moveToClosingDate {
                Source = <@
                    fun from _ prior _ ->
                        {
                            prior with
                                UnsmoothedAssetShare =
                                    from.apiCall (_.xl_PostOpeningRegression, _.Step4_MoveToClosingDate_UAS)
                                SmoothedAssetShare =
                                    from.apiCall (_.xl_PostOpeningRegression, _.Step4_MoveToClosingDate_SAS)
                        } @>

                Validator =
                    StepValidationOutcome.noValidator
            }
        )

    member val DataRollForward =
        this.registerInteriorStep (
            createStep.dataRollForward {
                DataChanger =
                    dataChanger_RollForward config.ClosingRunDate
                Validator =
                    StepValidationOutcome.noValidator
            }
        )

    member val Adjustments =
        this.registerInteriorStep (
            createStep.adjustments {
                Source = <@
                    fun from _ prior _ ->
                        {
                            prior with
                                UnsmoothedAssetShare =
                                    from.apiCall (_.xl_PostOpeningRegression, _.Step5_Adjustments_UAS)
                                SmoothedAssetShare =
                                    from.apiCall (_.xl_PostOpeningRegression, _.Step5_Adjustments_SAS)
                        } @>

                Validator =
                    StepValidationOutcome.noValidator
            }
        )

    member val Premiums =
        this.registerInteriorStep (
            createStep.premiums {
                Source = <@
                    fun from _ prior _ ->
                        {
                            prior with
                                UnsmoothedAssetShare =
                                    from.apiCall (_.xl_PostOpeningRegression, _.Step6_Premiums_UAS)
                                SmoothedAssetShare =
                                    from.apiCall (_.xl_PostOpeningRegression, _.Step6_Premiums_SAS)
                        } @>

                Validator =
                    StepValidationOutcome.noValidator
            }
        )

    member val Deductions =
        this.registerInteriorStep (
            createStep.deductions {
                Source = <@
                    fun from _ prior _ ->
                        {
                            prior with
                                UnsmoothedAssetShare =
                                    from.apiCall (_.xl_PostOpeningRegression, _.Step7_Deductions_UAS)
                                SmoothedAssetShare =
                                    from.apiCall (_.xl_PostOpeningRegression, _.Step7_Deductions_SAS)
                        } @>

                Validator =
                    StepValidationOutcome.noValidator
            }
        )

    member val MortalityCharges =
        this.registerInteriorStep (
            createStep.mortalityCharges {
                Source = <@
                    fun from _ prior _ ->
                        {
                            prior with
                                UnsmoothedAssetShare =
                                    from.apiCall (_.xl_PostOpeningRegression, _.Step8_MortalityCharge_UAS)
                                SmoothedAssetShare =
                                    from.apiCall (_.xl_PostOpeningRegression, _.Step8_MortalityCharge_SAS)
                        } @>

                Validator =
                    StepValidationOutcome.noValidator
            }
        )

    member val InvestmentReturns =
        this.registerInteriorStep (
            createStep.investmentReturns {
                Source = <@
                    fun from _ prior _ ->
                        {
                            prior with
                                UnsmoothedAssetShare =
                                    from.apiCall (_.xl_PostOpeningRegression, _.Step9_InvestmentReturn_UAS)
                                SmoothedAssetShare =
                                    from.apiCall (_.xl_PostOpeningRegression, _.Step9_InvestmentReturn_SAS)
                        } @>

                Validator =
                    StepValidationOutcome.noValidator
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
                        } @>

                Validator =
                    StepValidationOutcome.noValidator
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
                        } @>

                Validator =
                    StepValidationOutcome.noValidator
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
                        } @>

                Validator =
                    StepValidationOutcome.noValidator
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
                        } @>

                Validator =
                    StepValidationOutcome.noValidator
            }
        )

    member val RecentPaidUps  =
        this.registerInteriorStep (
            createStep.recentPaidUps {
                DataChanger =
                    dataChanger_RecentPaidUps
                Validator =
                    StepValidationOutcome.noValidator
            }
        )


    // --- REQUIRED STEPS ---

    override val MoveToClosingData =
        createStep.moveToClosingData {            
            Validator =
                StepValidationOutcome.noValidator
        }
 
    override val AddNewRecords =
        createStep.addNewRecords {
            Validator =
                StepValidationOutcome.noValidator
        }


    // --- POST NEW RECORDS STEPS ---

    // Used to quantify impact of latest returns.
    member val MoveToClosingMonthEnd =
        this.registerPostNewRecordsStep(
            createStep.moveToClosingMonthEnd {
                Source = <@
                    fun from _ _ _ ->
                        {                    
                            UnsmoothedAssetShare =
                                from.apiCall (_.xl_ClosingMonthEnd, _.UnsmoothedAssetShare)
                            SmoothedAssetShare =
                                from.apiCall (_.xl_ClosingMonthEnd, _.SmoothedAssetShare)
                            GuaranteedDeathBenefit =
                                from.apiCall (_.xl_ClosingMonthEnd, _.GuaranteedDeathBenefit)
                            ExitBonusRate =
                                from.apiCall (_.xl_ClosingMonthEnd, _.ExitBonusRate)
                            UnpaidPremiums =
                                from.apiCall (_.xl_ClosingMonthEnd, _.UnpaidPremiums)
                            SurrenderBenefit =
                                from.apiCall (_.xl_ClosingMonthEnd, _.CashSurrenderBenefit)
                            DeathUpliftFactor =
                                from.apiCall (_.xl_ClosingMonthEnd, _.DeathUpliftFactor)
                            DeathBenefit =
                                from.apiCall (_.xl_ClosingMonthEnd, _.DeathBenefit)
                        } @>

                Validator =
                    StepValidationOutcome.noValidator
            }
        )


    // Cannot make this val without leading to an initialization error.
    override this.ClosingStep =
        this.AddNewRecords

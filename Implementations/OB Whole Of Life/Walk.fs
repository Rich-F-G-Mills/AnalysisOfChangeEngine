
namespace AnalysisOfChangeEngine.Implementations.OBWholeOfLife

open System
open FsToolkit.ErrorHandling
open AnalysisOfChangeEngine.Common
open AnalysisOfChangeEngine.Implementations.Common


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
        X: int
    }


[<NoEquality; NoComparison>]
type ApiCollection =
    {
        px_OpeningRegression        : WrappedApiRequest<PolicyRecord, PxApi.OutputAttributes>
        px_PostOpeningRegression    : WrappedApiRequest<PolicyRecord, PxApi.OutputAttributes>
    }


[<Sealed>]
type Walk private (logger: ILogger, runContext: RunContext, config: WalkConfiguration) as this =
    inherit AbstractWalk<PolicyRecord, StepResults, ApiCollection> (logger)

    // --- DATA CHANGERS ---

    let dataChanger_RestatedOpening : PolicyRecordChanger<PolicyRecord> =
        fun (opening, _, closing) ->
            let restatedStatus =
                match opening.PolicyStatus, closing.PolicyStatus with
                | PolicyStatus.PremiumPaying, PolicyStatus.PaidUp ->
                    // Don't transition to PUP at this time.
                    PolicyStatus.PremiumPaying
                | _ ->
                    // It doesn't matter whether we pick that from the opening or closing!
                    closing.PolicyStatus                        

            let restated =
                {
                    opening with
                        TableCode = closing.TableCode
                        EntryDate = closing.EntryDate
                        SumAssured = closing.SumAssured
                        LivesBasis = closing.LivesBasis
                        PaymentTerm = closing.PaymentTerm
                        PolicyStatus = restatedStatus
                        // We intentionally don't change the NPDD at this time.
                }

            // If nothing has changed, return None as this will indicate to the
            // calling logic that nothing has changed for this record.
            if opening <> restated then Some restated else None
            
    let dataChanger_RollForward : PolicyRecordChanger<PolicyRecord> =
        fun (_, prior, closing) ->
            let updatedStatus, updatedNPDD =
                match prior.PolicyStatus, closing.PolicyStatus with
                | PolicyStatus.PremiumPaying, PolicyStatus.PaidUp ->
                    let rollForwardNPDD =
                        // Ensure that we at least roll it forward to the closing date.
                        DateOnly.Max (prior.NextPremiumDueDate, runContext.ClosingRunDate)

                    // We roll-forward the NPDD assuming the premium had been paid.
                    PolicyStatus.PremiumPaying, rollForwardNPDD

                | _ ->
                    // It doesn't matter whether we pick that from the opening or closing!
                    closing.PolicyStatus, closing.NextPremiumDueDate                       

            let updated =
                {
                    prior with
                        PolicyStatus = updatedStatus
                        NextPremiumDueDate = updatedNPDD
                }

            if prior <> updated then Some updated else None
            
    let dataChanger_NewPaidUps : PolicyRecordChanger<PolicyRecord> =
        fun (_, prior, closing) ->
            let updatedStatus, updatedNPDD =
                match prior.PolicyStatus, closing.PolicyStatus with
                | PolicyStatus.PremiumPaying, PolicyStatus.PaidUp ->
                    PolicyStatus.PaidUp, closing.NextPremiumDueDate
                | _ ->
                    prior.PolicyStatus, prior.NextPremiumDueDate

            let updated =
                {
                    prior with
                        PolicyStatus = updatedStatus
                        NextPremiumDueDate = updatedNPDD
                }

            if prior <> updated then Some updated else None


    // --- CONTROL CREATION ---

    static member create (logger, runContext, config) =
        result {
            return new Walk (logger, runContext, config)
        }


    // --- API CALLS ---

    member val px_OpeningRegression
        : WrappedApiRequest<_, PxApi.OutputAttributes> =
            PxApi.createOpeningDispatcher<PolicyRecord> {
                OpeningRunDate =
                    runContext.OpeningRunDate
            }

    member val px_PostOpeningRegression
        : WrappedApiRequest<_, PxApi.OutputAttributes> =
            PxApi.createPostOpeningDispatcher<PolicyRecord> {
                OpeningRunDate =
                    runContext.OpeningRunDate
                ClosingRunDate =
                    runContext.ClosingRunDate
            }




    // --- REQUIRED STEPS ---

    override val opening =
        StepTemplates.opening {
            Validator = noValidator
        }

    override val openingRegression =
        StepTemplates.openingRegression {
            Source =
                <@ fun from _ ->
                    {                    
                        UnsmoothedAssetShare =
                            from.apiCall (_.px_OpeningRegression , _.UnsmoothedAssetShare)
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
                    }
                @>

            Validator =
                fun (_, beforeResults, afterResults) ->
                    if beforeResults = afterResults then
                        List.empty
                    else
                        [ StepValidationIssue.Warning "Regression mis-match" ]
        }                

    override val removeExitedRecords =
        StepTemplates.removeExited 


    // --- PRODUCT SPECIFIC STEPS ---
    // We need to make sure that these are registered as well as defined!

    member val aocOpeningConsistencyCheck =
        // The interested ready may be wondering why this is needed...
        // As of the previous step, we're using the same UAS and SAS outputs as
        // underlying claims calculations. From this step onwards, we're using
        // UAS and SAS outputs as provided by PX's AoC machinery. In theory,
        // they should (!) always tie up. This just proves it.
        this.registerInteriorStep(        
            StepTemplates.aocOpeningConsistencyCheck {
                Source =
                    <@ fun from prior ->
                        {
                            prior with
                                UnsmoothedAssetShare =
                                    from.apiCall (_.px_OpeningRegression, _.Step0_Opening_UAS)
                                SmoothedAssetShare =
                                    from.apiCall (_.px_OpeningRegression, _.Step0_Opening_SAS)
                                SurrenderValue =
                                    from.calculation (fun x ->
                                        x.SmoothedAssetShare * (1.0 + x.ExitBonusRate) - x.UnpaidPremiums)
                                DeathBenefit =
                                    from.calculation (fun step ->
                                        Math.Max(
                                            step.SmoothedAssetShare * step.DeathUpliftFactor * (1.0 + step.ExitBonusRate),
                                            step.GuaranteedDeathBenefit
                                        ) - step.UnpaidPremiums)
                        }
                    @>

                Validator =
                    noValidator
            }
        )

    member val restatedOpeningData =
        this.registerInteriorStep(
            StepTemplates.restatedOpeningData {
                DataChanger =
                    dataChanger_RestatedOpening
                Validator =
                    noValidator            
            }
        )    
            

    member val restatedOpeningAdjustments = 0

    member val restatedOpeningReturns =
        this.registerInteriorStep(
            StepTemplates.restatedOpeningReturns {
                Source =
                    <@ fun from prior ->
                        {
                            prior with
                                UnsmoothedAssetShare =
                                    from.apiCall (_.px_PostOpeningRegression, _.Step2_RestatedActuals_UAS)
                                SmoothedAssetShare =
                                    from.apiCall (_.px_PostOpeningRegression, _.Step2_RestatedActuals_SAS)
                        }
                    @>

                Validator =
                    noValidator
            }
        )
            
    member val restatedOpeningDeductions =
        this.registerInteriorStep(
            StepTemplates.restatedOpeningDeductions {
                Source =
                    <@ fun from prior ->
                        {
                            prior with
                                UnsmoothedAssetShare =
                                    from.apiCall (_.px_PostOpeningRegression, _.Step3_RestatedDeductions_UAS)
                                SmoothedAssetShare =
                                    from.apiCall (_.px_PostOpeningRegression, _.Step3_RestatedDeductions_SAS)
                        }
                    @>

                Validator =
                    noValidator
            }
        )      


    // --- REQUIRED STEPS ---

    override val moveToClosingExistingData =
        StepTemplates.moveToClosingExistingData {            
            Validator =
                noValidator
        }
 
    override val addNewRecords =
        StepTemplates.addNewRecords {
            Validator = noValidator
        }
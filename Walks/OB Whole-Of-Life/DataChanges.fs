
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
open AnalysisOfChangeEngine.Walks.OBWholeOfLife


[<AutoOpen>]
module internal DataChanges =

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
            
    let dataChanger_RollForward closingRunDate
        : PolicyRecordChanger<_> =
            fun (_, OBWholeOfLife.PolicyRecord prior, OBWholeOfLife.PolicyRecord closing) ->
                result {
                    let updatedStatus, updatedNPDD =
                        match prior.Status, closing.Status with
                        | OBWholeOfLife.PolicyStatus.PremiumPaying, OBWholeOfLife.PolicyStatus.PaidUp ->
                            let rollForwardNPDD =
                                // Ensure that we at least roll it forward to the closing date.
                                DateOnly.Max (prior.NextPremiumDueDate, closingRunDate)

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
    
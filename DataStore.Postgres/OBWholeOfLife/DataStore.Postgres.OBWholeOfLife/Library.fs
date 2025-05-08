
namespace AnalysisOfChangeEngine.DataStore.Postgres


[<RequireQualifiedAccess>]
module OBWholeOfLife =

    open System
    open Npgsql
    open AnalysisOfChangeEngine.Structures.PolicyRecords
    open AnalysisOfChangeEngine.Structures.StepResults


    [<NoComparison; NoEquality; PostgresEnumeration("policy_status")>]
    type PolicyStatusDTO =
        | PP
        | PUP
        | AP


    [<NoComparison; NoEquality; PostgresEnumeration("gender")>]
    type GenderDTO =
        | MALE
        | FEMALE


    [<NoComparison; NoEquality>]
    type PolicyRecordDTO =
        {
            policy_id           : string
            table_code          : string
            status              : PolicyStatusDTO
            sum_assured         : float32
            entry_date          : DateOnly
            npdd                : DateOnly
            ltd_payment_term    : Int16     // smallint
            entry_age_1         : Int16
            gender_1            : GenderDTO
            entry_age_2         : Int16 option
            gender_2            : GenderDTO option
            joint_val_age       : Int16 option
        }


    [<NoComparison; NoEquality>]
    type StepResultsDTO =
        {
            unsmoothed_asset_share      : float32
            smoothed_asset_share        : float32
            guaranteed_death_benefit    : float32
            surrender_benefit           : float32
            death_benefit               : float32
            exit_bonus_rate             : float32
            unpaid_premiums             : float32
            death_uplift_factor         : float32
        }



    type DataStore (sessionContext: SessionContext, connection: NpgsqlConnection) =
        inherit AbstractDataStore<OBWholeOfLife.PolicyRecord, PolicyRecordDTO, OBWholeOfLife.StepResults, StepResultsDTO>
            (sessionContext, connection, "ob_wol")

        override _.policyRecordToDto policyRecord =
            {
                policy_id           = policyRecord.PolicyId
                table_code          = policyRecord.TableCode
                status              =
                    match policyRecord.Status with
                    | OBWholeOfLife.PolicyStatus.PremiumPaying  -> PolicyStatusDTO.PP
                    | OBWholeOfLife.PolicyStatus.PaidUp         -> PolicyStatusDTO.PUP
                    | OBWholeOfLife.PolicyStatus.AllPaid        -> PolicyStatusDTO.AP
                sum_assured         = policyRecord.SumAssured
                entry_date          = policyRecord.EntryDate
                npdd                = policyRecord.NextPremiumDueDate
                ltd_payment_term    = policyRecord.LimitedPaymentTerm
                entry_age_1         = Int16 policyRecord.Lives.EntryAgeLife1
                entry_age_2         =
                    policyRecord.Lives.EntryAgeLife2
                    |> Option.map Int16
                gender_1            =
                    match policyRecord.Lives.GenderLife1 with
                    | OBWholeOfLife.Gender.Male     -> GenderDTO.MALE
                    | OBWholeOfLife.Gender.Female   -> GenderDTO.FEMALE
                gender_2
            
            }

        override _.dtoToStepResults dto =
            Ok {
                UnsmoothedAssetShare        = dto.unsmoothed_asset_share
                SmoothedAssetShare          = dto.smoothed_asset_share
                GuaranteedDeathBenefit      = dto.guaranteed_death_benefit
                SurrenderBenefit            = dto.surrender_benefit
                DeathBenefit                = dto.death_benefit
                ExitBonusRate               = dto.exit_bonus_rate
                UnpaidPremiums              = dto.unpaid_premiums
                DeathUpliftFactor           = dto.death_uplift_factor
            }

        override _.stepResultsToDto stepResults =
            Ok {
                unsmoothed_asset_share      = stepResults.UnsmoothedAssetShare
                smoothed_asset_share        = stepResults.SmoothedAssetShare
                guaranteed_death_benefit    = stepResults.GuaranteedDeathBenefit
                surrender_benefit           = stepResults.SurrenderBenefit
                death_benefit               = stepResults.DeathBenefit
                exit_bonus_rate             = stepResults.ExitBonusRate
                unpaid_premiums             = stepResults.UnpaidPremiums
                death_uplift_factor         = stepResults.DeathUpliftFactor
            }           



namespace AnalysisOfChangeEngine.DataStore.Postgres


[<RequireQualifiedAccess>]
module OBWholeOfLife =

    open System
    open Npgsql
    open FsToolkit.ErrorHandling
    open AnalysisOfChangeEngine.Structures.PolicyRecords
    open AnalysisOfChangeEngine.Structures.StepResults


    let [<Literal>] private schema = "ob_wol"


    [<RequireQualifiedAccess; NoComparison; NoEquality>]
    [<PostgresProductSpecificEnumeration("gender")>]
    type GenderDTO =
        | MALE
        | FEMALE

    [<RequireQualifiedAccess>]
    module GenderDTO =

        let ofGender = function
            | OBWholeOfLife.Gender.Male ->
                GenderDTO.MALE
            | OBWholeOfLife.Gender.Female ->
                GenderDTO.FEMALE

        let toGender = function
            | GenderDTO.MALE ->
                OBWholeOfLife.Gender.Male
            | GenderDTO.FEMALE ->
                OBWholeOfLife.Gender.Female


    [<RequireQualifiedAccess; NoComparison; NoEquality>]
    [<PostgresProductSpecificEnumeration("policy_status")>]
    type PolicyStatusDTO =
        | PP
        | PUP
        | AP

    [<RequireQualifiedAccess>]
    module PolicyStatusDTO =

        let ofPolicyStatus = function
            | OBWholeOfLife.PolicyStatus.PremiumPaying ->
                PolicyStatusDTO.PP
            | OBWholeOfLife.PolicyStatus.PaidUp ->
                PolicyStatusDTO.PUP
            | OBWholeOfLife.PolicyStatus.AllPaid ->
                PolicyStatusDTO.AP

        let toPolicyStatus = function
            | PolicyStatusDTO.PP ->
                OBWholeOfLife.PolicyStatus.PremiumPaying
            | PolicyStatusDTO.PUP ->
                OBWholeOfLife.PolicyStatus.PaidUp
            | PolicyStatusDTO.AP ->
                OBWholeOfLife.PolicyStatus.AllPaid


    [<RequireQualifiedAccess; NoComparison; NoEquality>]
    [<PostgresProductSpecificEnumeration("premium_frequency")>]
    type PremiumFrequencyDTO =
        | MONTHLY
        | YEARLY

    [<RequireQualifiedAccess>]
    module PremiumFrequencyDTO = 

        let ofPremiumFrequency = function
            | OBWholeOfLife.PremiumFrequency.Monthly ->
                PremiumFrequencyDTO.MONTHLY
            | OBWholeOfLife.PremiumFrequency.Yearly ->
                PremiumFrequencyDTO.YEARLY

        let toPremiumFrequency = function
            | PremiumFrequencyDTO.MONTHLY ->
                OBWholeOfLife.PremiumFrequency.Monthly
            | PremiumFrequencyDTO.YEARLY ->
                OBWholeOfLife.PremiumFrequency.Yearly


    [<NoComparison; NoEquality>]
    type PolicyRecordDTO =
        {
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
            premium_frequency   : PremiumFrequencyDTO
            modal_premium       : float32
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


    [<NoComparison; NoEquality>]
    type private TableCodeDTO =
        {
            table_code                  : string
            is_taxable                  : bool
        }

    [<RequireQualifiedAccess>]
    module private TableCodeDTO =

        type internal IDispatcher =
            interface
                abstract member SelectAll       : unit -> TableCodeDTO list
            end

        let internal buildDispatcher dataSource =
            let dispatcher =
                new DataTransferObjects.PostgresTableDispatcher<TableCodeDTO, unit>
                    ("table_codes", schema, dataSource)

            {
                new IDispatcher with
                    member _.SelectAll () =
                        dispatcher.SelectAllBaseRecords ()
            }


    type DataStore (sessionContext: SessionContext, dataSource: NpgsqlDataSource) =
        inherit AbstractPostgresDataStore<OBWholeOfLife.PolicyRecord, PolicyRecordDTO, OBWholeOfLife.StepResults, StepResultsDTO>
            (sessionContext, dataSource, schema)

        let tableCodeDispatcher =
            TableCodeDTO.buildDispatcher (dataSource)

        let tableCodes =
            tableCodeDispatcher.SelectAll ()
            |> Seq.map (fun tc -> tc.table_code, tc)
            |> Map.ofSeq

        override _.dtoToPolicyRecord policyRecord =
            result {
                let firstLife: OBWholeOfLife.LifeData =
                    {
                        EntryAge = int policyRecord.entry_age_1
                        Gender = GenderDTO.toGender policyRecord.gender_1
                    }

                let! lives =
                    match policyRecord.entry_age_2, policyRecord.gender_2, policyRecord.joint_val_age with
                    | None, None, None ->
                        Ok (OBWholeOfLife.LivesBasis.SingleLife firstLife)

                    | Some entry_age_2', Some gender_2', Some joint_val_age' ->
                        let secondLife: OBWholeOfLife.LifeData =
                            {
                                EntryAge = int entry_age_2'
                                Gender = GenderDTO.toGender gender_2'
                            }

                        Ok (OBWholeOfLife.LivesBasis.JointLife (firstLife, secondLife, int joint_val_age'))

                    | _ ->
                        Error "Invalid combination of life data provided."

                let! isTaxable =
                    tableCodes
                    |> Map.tryFind policyRecord.table_code
                    |> Option.map _.is_taxable
                    |> Result.requireSome
                        (sprintf "Table code %s not found in table_codes." policyRecord.table_code)

                let rawPolicyRecord: OBWholeOfLife.RawPolicyRecord =
                    {                
                        TableCode           = policyRecord.table_code
                        Taxable             = isTaxable
                        EntryDate           = policyRecord.entry_date
                        NextPremiumDueDate  = policyRecord.npdd
                        Status              =
                            PolicyStatusDTO.toPolicyStatus policyRecord.status
                        Lives               = lives
                        SumAssured          = policyRecord.sum_assured
                        LimitedPaymentTerm  = int policyRecord.ltd_payment_term
                        PremiumFrequency    =
                            PremiumFrequencyDTO.toPremiumFrequency policyRecord.premium_frequency
                        ModalPremium        = policyRecord.modal_premium
                    }

                return! OBWholeOfLife.PolicyRecord.validate rawPolicyRecord
            }
        
        override _.policyRecordToDTO (OBWholeOfLife.PolicyRecord policyRecord) =
            Ok {
                table_code          = policyRecord.TableCode
                status              =
                    PolicyStatusDTO.ofPolicyStatus policyRecord.Status
                sum_assured         = policyRecord.SumAssured
                entry_date          = policyRecord.EntryDate
                npdd                = policyRecord.NextPremiumDueDate
                ltd_payment_term    = int16 policyRecord.LimitedPaymentTerm
                entry_age_1         = int16 policyRecord.Lives.EntryAgeLife1
                entry_age_2         =
                    policyRecord.Lives.EntryAgeLife2
                    |> Option.map int16
                gender_1            =
                    GenderDTO.ofGender policyRecord.Lives.GenderLife1
                gender_2 =
                    policyRecord.Lives.GenderLife2
                    |> Option.map GenderDTO.ofGender
                joint_val_age =
                    policyRecord.Lives.JointValuationAge
                    |> Option.map int16
                premium_frequency   =
                    PremiumFrequencyDTO.ofPremiumFrequency policyRecord.PremiumFrequency
                modal_premium       = policyRecord.ModalPremium
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

        override _.stepResultsToDTO stepResults =
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

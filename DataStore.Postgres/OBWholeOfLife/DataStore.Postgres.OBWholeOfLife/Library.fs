
namespace AnalysisOfChangeEngine.DataStore.Postgres


[<RequireQualifiedAccess>]
module OBWholeOfLife =

    open System
    open Npgsql
    open AnalysisOfChangeEngine.Structures.PolicyRecords
    open AnalysisOfChangeEngine.Structures.StepResults




    type DataStore (sessionContext: SessionContext, connection: NpgsqlConnection) =
        inherit AbstractDataStore<OBWholeOfLife.PolicyRecord, OBWholeOfLife.StepResults>
            (sessionContext, connection, "ob_wol")

        override _.parseStepResultsRow (row: RowReader) =
            Ok {
                UnsmoothedAssetShare    = row.double "unsmoothed_asset_share"
                SmoothedAssetShare      = row.double "smoothed_asset_share"
                GuaranteedDeathBenefit  = row.double "guaranteed_death_benefit"
                SurrenderBenefit        = row.double "surrender_benefit"
                DeathBenefit            = row.double "death_benefit"
                ExitBonusRate           = row.double "exit_bonus_rate"
                UnpaidPremiums          = row.double "unpaid_premiums"
                DeathUpliftFactor       = row.double "death_uplift_factor"
            }

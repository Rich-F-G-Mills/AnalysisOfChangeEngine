
con <-
  DBI::dbConnect(
    RPostgres::Postgres(),
    dbname = 'analysis_of_change', 
    host = 'localhost',
    port = 5432L,
    user = 'postgres',
    password = 'internet',
    options = '-c search_path=common'
  )

stepHeaders <-
  dplyr::tbl(con, 'step_headers') |>
  dplyr::mutate(
    type = as.character(type)
  ) |>
  dplyr::collect()

DBI::dbDisconnect(con)


# Connect to a specific postgres database i.e. Heroku
con <-
  DBI::dbConnect(
    RPostgres::Postgres(),
    dbname = 'analysis_of_change', 
    host = 'localhost',
    port = 5432L,
    user = 'postgres',
    password = 'internet',
    options = '-c search_path=ob_wol'
  )

sessionUids <-
  dplyr::tbl(con, 'step_results') |>
  dplyr::filter(run_uid == params$currentRunUid) |>
  dplyr::distinct(session_uid) |>
  dplyr::collect() |>
  dplyr::pull(session_uid)

curatedStepResults <-
  dplyr::tbl(con, 'curated_step_results') |>
  dplyr::filter(run_uid == params$currentRunUid) |>
  dplyr::collect()

curatedStepDeltas <-
  dplyr::tbl(con, 'curated_step_deltas') |>
  dplyr::filter(run_uid == params$currentRunUid) |>
  dplyr::collect()

policyTracing <-
  dplyr::tbl(con, 'policy_tracing') |>
  dplyr::filter(
    run_uid == params$currentRunUid
  ) |>
  dplyr::collect()
  
DBI::dbDisconnect(con)

rm(con)

curatedStepResults <-
  curatedStepResults |>
  dplyr::left_join(
    stepHeaders |>
      dplyr::select(
        step_uid,
        title,
        type
      ),
    by = 'step_uid'
  ) %T>%
  readr::write_csv(
    file = 'OUTPUTS/STEP_RESULTS.CSV'
  )

curatedStepDeltas <-
  curatedStepDeltas |>
  dplyr::left_join(
    stepHeaders |>
      dplyr::select(
        step_uid,
        title,
        type
      ),
    by = 'step_uid'
  ) %T>%
  readr::write_csv(
    file = 'OUTPUTS/STEP_DELTAS.CSV'
  )



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

runHeader <-
  dplyr::tbl(con, 'run_headers') |>
  dplyr::filter(uid == currentRunUid) |>
  dplyr::collect()

priorRunHeader <-
  dplyr::tbl(con, 'run_headers') |>
  dplyr::filter(uid == runHeader$prior_run_uid) |>
  dplyr::collect()

runSteps <-
  dplyr::tbl(con, 'run_steps') |>
  dplyr::collect()

stepResults <-
  dplyr::tbl(con, 'step_results') |>
  dplyr::filter(run_uid == currentRunUid)

openingPolicyData <-
  dplyr::tbl(con, 'policy_data') |>
  dplyr::filter(extraction_uid == priorRunHeader$policy_data_extraction_uid) |>
  dplyr::semi_join(stepResults, by = 'policy_id') |>
  dplyr::collect() |>
  dplyr::select(-extraction_uid)

closingPolicyData <-
  dplyr::tbl(con, 'policy_data') |>
  dplyr::filter(extraction_uid == runHeader$policy_data_extraction_uid) |>
  dplyr::semi_join(stepResults, by = 'policy_id') |>
  dplyr::collect() |>
  dplyr::select(-extraction_uid)

dataStages <-
  dplyr::tbl(con, 'data_stages') |>
  dplyr::filter(run_uid == currentRunUid) |>
  dplyr::semi_join(stepResults, by = 'policy_id') |>
  dplyr::collect() |>
  dplyr::select(-run_uid)
  
DBI::dbDisconnect(con)

rm(con)
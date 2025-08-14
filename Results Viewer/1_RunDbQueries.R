
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

dataStages <-
  dplyr::tbl(con, 'data_stages') |>
  dplyr::filter(run_uid == currentRunUid) |>
  dplyr::collect()

stepResults <-
  dplyr::tbl(con, 'step_results') |>
  dplyr::filter(run_uid == currentRunUid) |>
  dplyr::collect()
  

DBI::dbDisconnect(con)

rm(con)
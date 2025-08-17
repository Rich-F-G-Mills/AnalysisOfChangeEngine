
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

runHeader <-
  dplyr::tbl(con, 'run_headers') |>
  dplyr::filter(uid == params$currentRunUid) |>
  dplyr::collect()

priorRunHeader <-
  dplyr::tbl(con, 'run_headers') |>
  dplyr::filter(uid == runHeader$prior_run_uid) |>
  dplyr::collect()

runSteps <-
  dplyr::tbl(con, 'run_steps') |>
  dplyr::filter(run_uid == params$currentRunUid) |>
  dplyr::collect()

stepResults <-
  dplyr::tbl(con, 'step_results') |>
  dplyr::filter(run_uid == params$currentRunUid)

openingPolicyData <-
  dplyr::tbl(con, 'policy_data') |>
  dplyr::filter(extraction_uid == priorRunHeader$policy_data_extraction_uid) |>
  dplyr::semi_join(stepResults, by = 'policy_id') |>
  dplyr::select(-extraction_uid) |>
  dplyr::collect()

closingPolicyData <-
  dplyr::tbl(con, 'policy_data') |>
  dplyr::filter(extraction_uid == runHeader$policy_data_extraction_uid) |>
  dplyr::semi_join(stepResults, by = 'policy_id') |>
  dplyr::select(-extraction_uid) |>
  dplyr::collect()

dataStages <-
  dplyr::tbl(con, 'data_stages') |>
  dplyr::filter(run_uid == params$currentRunUid) |>
  dplyr::semi_join(stepResults, by = 'policy_id') |>
  dplyr::select(
    -run_uid,
    -session_uid
  ) |>
  dplyr::collect()

stepResults <-
  stepResults |>
  dplyr::collect()
  
DBI::dbDisconnect(con)

rm(con)


sessionUids <-
  unique(stepResults$session_uid)

stepResults <-
  stepResults |>
  dplyr::select(
    -run_uid,
    -session_uid
  )

stepResultItems <-
  names(stepResults) |>
  setdiff(c('policy_id', 'step_uid'))

augmentedRunSteps <-
  runSteps |>
  dplyr::left_join(
    stepHeaders,
    by = c('step_uid' = 'uid')
  ) |>
  dplyr::arrange(
    step_idx
  ) |>
  dplyr::select(
    -run_uid
  )

rm(runSteps)

combinedPolicyData <-
  openingPolicyData |>
  dplyr::mutate(
    data_stage_uid =
      params$openingDataStageUid
  ) |>
  dplyr::bind_rows(
    closingPolicyData |>
      dplyr::mutate(
        data_stage_uid =
          params$closingDataStageUid
      )
  ) |>
  dplyr::bind_rows(
    dataStages
  ) |>
  dplyr::mutate(
    dplyr::across(
      dplyr::where(
        ~ stringr::str_detect(class(.), '^pq_')
      ),
      as.character
    )
  )

policyDataItems <-
  names(combinedPolicyData) |>
  setdiff(c('policy_id', 'data_stage_uid'))

policyIdCohorts <-
  combinedPolicyData |>
  dplyr::distinct(
    policy_id
  ) |>
  dplyr::mutate(
    in_opening_data =
      policy_id %in% openingPolicyData$policy_id,
    in_closing_data =
      policy_id %in% closingPolicyData$policy_id
  )

augmentedStepResults <-
  policyIdCohorts |>
  dplyr::cross_join(
    augmentedRunSteps |>
      dplyr::select(
        step_idx,
        step_uid,
        run_if_exited_record,
        run_if_new_record
      )
  ) |>
  dplyr::filter(
    (in_opening_data & run_if_exited_record)
    | (in_closing_data & run_if_new_record)
    | (in_opening_data & in_closing_data)
  ) |>
  dplyr::mutate(
    policy_id,
    step_idx,
    step_uid,
    expected_step =
      TRUE
  ) |>
  dplyr::full_join(
    stepResults |>
      dplyr::mutate(
        performed_step =
          TRUE
      ),
    by = c('policy_id', 'step_uid')
  ) |>
  dplyr::mutate(
    expected_step =
      tidyr::replace_na(expected_step, FALSE),
    performed_step =
      tidyr::replace_na(performed_step, FALSE)
  ) |>
  dplyr::left_join(
    combinedPolicyData,
    by = c('policy_id', 'used_data_stage_uid' = 'data_stage_uid')
  ) |>
  dplyr::select(
    policy_id,
    step_idx,
    step_uid,
    used_data_stage_uid,
    tidyselect::any_of(names(combinedPolicyData)),
    tidyselect::any_of(names(stepResults))
  )

augmentedStepImpacts <-
  augmentedStepResults |>
  dplyr::mutate(
    prior_step_idx =
      step_idx - 1L
  ) |>
  dplyr::filter(
    prior_step_idx >= 0L
  ) |>
  dplyr::left_join(
    augmentedStepResults |>
      dplyr::select(
        -step_uid
      ) |>
      dplyr::rename_with(
        ~ glue::glue('prior_{.}'),
        c(-policy_id, -step_idx)
      ),
    by = c('policy_id', 'prior_step_idx' = 'step_idx')
  ) |>
  dplyr::select(
    -prior_step_idx
  )
  


params <-
  list(
    run_uid =
      '40b70255-1c18-488f-821c-492a2835c810',
    opening_data_uid =
      '00000000-0000-0000-0000-000000000000',
    closing_data_uid =
      'ffffffff-ffff-ffff-ffff-ffffffffffff'
  )


step_headers <-
  local({
    db_common <-
      DBI::dbConnect(
        RPostgres::Postgres(),
        dbname = 'analysis_of_change', 
        host = 'localhost',
        port = 5432L,
        user = 'postgres',
        password = 'internet',
        options = '-c search_path=common'
      )
    
    step_headers <-
      dplyr::tbl(db_common, 'step_headers') |>
      dplyr::collect()
    
    DBI::dbDisconnect(db_common)
    
    step_headers
  })

db_ob_wol <-
  DBI::dbConnect(
    RPostgres::Postgres(),
    dbname = 'analysis_of_change', 
    host = 'localhost',
    port = 5432L,
    user = 'postgres',
    password = 'internet',
    options = '-c search_path=ob_wol'
  )


all_run_headers <-
  dplyr::tbl(db_ob_wol, 'run_headers') |>
  dplyr::collect()

run_details <-
  local({
    current_run_header <-
      all_run_headers |>
      dplyr::filter(
        run_uid == params$run_uid
      )
    
    prior_run_header <-
      all_run_headers |>
      dplyr::filter(
        run_uid == current_run_header$prior_run_uid
      )
    
    session_uids <-
      dplyr::tbl(db_ob_wol, 'curated_step_results') |>
      dplyr::filter(
        run_uid == params$run_uid,
        # Ignore the session UID for the prior closing step which
        # will be NULL anyway.
        step_idx > 0L
      ) |>
      dplyr::distinct(
        session_uid
      ) |>
      dplyr::collect()
    
    list(
      run_uid =
        params$run_uid,
      opening_run_date =
        prior_run_header$closing_run_date,
      closing_run_date =
        current_run_header$closing_run_date,
      prior_run_uid =
        prior_run_header$run_uid,
      current_data_extraction_uid =
        current_run_header$policy_data_extraction_uid,
      prior_data_extraction_uid =
        prior_run_header$policy_data_extraction_uid,
      prior_closing_run_step_uid =
        prior_run_header$closing_step_uid,
      session_uids =
        session_uids
    )
  })


step_metrics <-
  c(
    'unsmoothed_asset_share',
    'smoothed_asset_share',
    'surrender_benefit',
    'death_benefit',
    'exit_bonus_rate',
    'unpaid_premiums',
    'death_uplift_factor',
    'guaranteed_death_benefit'
  )

opening_policy_data <-
  dplyr::tbl(db_ob_wol, 'policy_data') |>
  dplyr::filter(
    extraction_uid == run_details$prior_data_extraction_uid
  ) |>
  dplyr::select(
    -extraction_uid
  ) |>
  dplyr::collect() |>
  dplyr::mutate(
    dplyr::across(
      dplyr::one_of('gender_1', 'gender_2', 'premium_frequency'),
      as.character
    )
  )

closing_policy_data <-
  dplyr::tbl(db_ob_wol, 'policy_data') |>
  dplyr::filter(
    extraction_uid == run_details$current_data_extraction_uid
  ) |>
  dplyr::select(
    -extraction_uid
  ) |>
  dplyr::collect() |>
  dplyr::mutate(
    dplyr::across(
      dplyr::one_of('gender_1', 'gender_2', 'premium_frequency'),
      as.character
    )
  )

step_results <-
  dplyr::tbl(db_ob_wol, 'curated_step_results') |>
  dplyr::filter(
    run_uid == params$run_uid
  ) |>
  dplyr::select(
    session_uid,
    policy_id,
    step_uid,
    step_idx,
    used_data_stage_uid,
    tidyselect::one_of(step_metrics)
  ) |>
  dplyr::collect()

DBI::dbDisconnect(db_ob_wol)


json_opening_policy_data <-
  opening_policy_data |>
  dplyr::mutate(
    dplyr::across(
      dplyr::where(lubridate::is.Date),
      \(x) format(x, "new Data(%Y, %m, %d)")
    )
  ) |>
  dplyr::transmute(
    json =
      glue::glue(
        " '{policy_id}': {{
            table_code: '{table_code}',
            status: '{status}',
            sum_assured: {sum_assured},
            entry_date: {entry_date},
            npdd: {entry_date},
            ltd_payment_term: {ltd_payment_term},
            entry_age_1: {entry_age_1},
            gender_1: '{gender_1}',
            entry_age_2: {entry_age_2},
            gender_2: '{gender_2}',
            joint_val_age: {joint_val_age},
            premium_frequency: '{premium_frequency}',
            modal_premium: {modal_premium}
          }}"
      ) |>
      stringr::str_replace_all(
        pattern = stringr::fixed("'NA'"),
        replacement = "null"
      )|>
      stringr::str_replace_all(
        pattern = stringr::fixed("NA"),
        replacement = "null"
      )
  )
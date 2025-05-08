
library (lubridate)


params <-
  list(
    openingRunUid             = '7526621f-74c3-4edf-8fe8-bebb20c36cd3',
    closingRunUid             = '7baaafdb-88f0-4d64-b8a6-99040e919307',
    openingExtractionDate     = lubridate::ymd('2025-01-01'),
    closingExtractionDate     = lubridate::ymd('2025-02-01'),
    openingExtractionUid      = '3f1a56c8-9d23-42d7-a5b1-874f01b87e1f',
    closingExtractionUid      = 'd49cb0ab-79e9-4b39-bc0d-47ae8b19e092',
    numPolicies               = 10000L,
    probExit                  = 0.05,
    probReinstate             = 0.025,
    probEntryDateChange       = 0.002,
    probOpeningPUP            = 0.4,
    probBecomesPUP            = 0.05,
    probIsJointLife           = 0.15
  )


policyDetails <-
  with(
    params,
    tibble::tibble(
      POLICY_ID =
        runif(
          n = as.integer(numPolicies * 1.5),
          min = 0,
          max = 1e8
        ) |>
        as.integer() |>
        sprintf(
          fmt = "P%08d"
        )
    ) |>
    dplyr::distinct() |>
    dplyr::slice_head(n = numPolicies) |>
    dplyr::mutate(
      TABLE_CODE =
        sample(
          c('T65', 'T68', 'T70', 'T75'),
          size = numPolicies,
          replace = TRUE
        ),
      
      POLICY_FATE =
        sample(
          c('EXIT', 'REINSTATE', 'REMAIN'),
          size = numPolicies,
          replace = TRUE,
          prob =
            c(probExit, probReinstate, 1.0 - probExit - probReinstate)
        ),
      
      RANDOM_ENTRY_DATE_OFFSET =
        runif(n = numPolicies, min = 0.0, max = 20 * 365) |>
        as.integer() |>
        days(),
      
      ENTRY_DATE =
        lubridate::ymd('1980-01-01') + RANDOM_ENTRY_DATE_OFFSET,
      
      SUM_ASSURED =
        runif(n = numPolicies, min = 100.0, max = 100000.0) |>
        round(digits = 2L),
      
      LTD_PAYMENT_TERM =
        runif(n = numPolicies, min = 15L, max = 60L) |>
        as.integer(),
      
      ALL_PAID_DATE =
        ENTRY_DATE %m+% years(LTD_PAYMENT_TERM),
      
      IS_ALL_PAID_AT_OPENING =
        ALL_PAID_DATE <= openingExtractionDate,
      
      IS_ALL_PAID_AT_CLOSING =
        ALL_PAID_DATE <= closingExtractionDate,
      
      OPENING_STATUS =
        dplyr::if_else(
          IS_ALL_PAID_AT_OPENING,
          'AP',
          sample(
            c('PUP', 'PP'),
            size = numPolicies,
            replace = TRUE,
            prob = c(probOpeningPUP, 1.0 - probOpeningPUP) 
          )
        ),
      
      TRANSITION_TO_PUP =
        runif(n = numPolicies, min = 0.0, max = 1.0) |>
        magrittr::is_less_than(probBecomesPUP),
      
      CLOSING_STATUS =
        dplyr::case_when(
          OPENING_STATUS == 'PP' & IS_ALL_PAID_AT_CLOSING ~ 'AP',
          OPENING_STATUS == 'PP' & TRANSITION_TO_PUP ~ 'PUP',
          TRUE ~ OPENING_STATUS
        ),
      
      MONTHS_IN_FORCE =
        lubridate::interval(
          ENTRY_DATE,
          openingExtractionDate
        ) |>
        lubridate::time_length(
          unit = 'month'
        ),
      
      PP_MONTHS =
        runif(n = numPolicies, min = 0.1, max = 0.9) |>
        magrittr::multiply_by(MONTHS_IN_FORCE) |>
        as.integer(),
      
      OPENING_NPDD =
        dplyr::case_match(
          OPENING_STATUS,
          'PP' ~ openingExtractionDate,
          'PUP' ~ ENTRY_DATE %m+% months(PP_MONTHS),
          'AP' ~ ALL_PAID_DATE
        ),
      
      CLOSING_NPDD =
        dplyr::case_when(
          OPENING_STATUS == 'PP' & CLOSING_STATUS == 'PUP' ~ openingExtractionDate,
          CLOSING_STATUS == 'PUP' ~ OPENING_NPDD,
          CLOSING_STATUS == 'PP' ~ closingExtractionDate,
          CLOSING_STATUS == 'AP' ~ ALL_PAID_DATE
        ),
      
      IS_JOINT_LIFE =
        runif(n = numPolicies, min = 0.0, max = 1.0) |>
        magrittr::is_less_than(probIsJointLife),
      
      GENDER_1 =
        sample(
          c('MALE', 'FEMALE'),
          size = numPolicies,
          replace = TRUE,
          prob = c(0.5, 0.5)
        ),
      
      ENTRY_AGE_1 =
        75L - LTD_PAYMENT_TERM,

      GENDER_2 =
        dplyr::if_else(
          IS_JOINT_LIFE,
          sample(
            c('MALE', 'FEMALE'),
            size = numPolicies,
            replace = TRUE,
            prob = c(0.5, 0.5)
          ),
          NA_character_
        ),

      ENTRY_AGE_2 =
        dplyr::if_else(
          IS_JOINT_LIFE,
          ENTRY_AGE_1 + 3L,
          NA_integer_
        ),
      
      JVA =
        dplyr::if_else(
          IS_JOINT_LIFE,
          as.integer(2.0 + (ENTRY_AGE_1 + ENTRY_AGE_2) / 2),
          NA_integer_
        )
    ) |>
    dplyr::select(
      -ALL_PAID_DATE,
      -IS_ALL_PAID_AT_OPENING,
      -IS_ALL_PAID_AT_CLOSING,
      -RANDOM_ENTRY_DATE_OFFSET,
      -TRANSITION_TO_PUP,
      -MONTHS_IN_FORCE,
      -PP_MONTHS,
      -IS_JOINT_LIFE
    )
  )


policyDetails |>
  saveRDS(
    file = 'OB_WOL_POLICY_DATA.RDS'
  )


generatedSql <-
  with(
    params,
    policyDetails |>
    dplyr::transmute(
      POLICY_ID,
      POLICY_FATE,
      OPENING_SQL =
        glue::glue(
          .na = 'NULL',
          "(
              '{openingExtractionUid}',
              '{POLICY_ID}',
              '{TABLE_CODE}',
              '{OPENING_STATUS}',
              {SUM_ASSURED},
              '{ENTRY_DATE}',
              '{OPENING_NPDD}',
              {LTD_PAYMENT_TERM},
              {ENTRY_AGE_1},
              '{GENDER_1}',
              {ENTRY_AGE_2},
              '{GENDER_2}',
              {JVA}
            )"
        ) |>
        stringr::str_remove_all(
          pattern = '[\n\r]'
        ) |>
        stringr::str_replace_all(
          pattern =
            stringr::fixed("'NULL'"),
          replacement = 
            'NULL'
        ),
      
      CLOSING_SQL =
        glue::glue(
          .na = 'NULL',
          "(
              '{closingExtractionUid}',
              '{POLICY_ID}',
              '{TABLE_CODE}',
              '{CLOSING_STATUS}',
              {SUM_ASSURED},
              '{ENTRY_DATE}',
              '{CLOSING_NPDD}',
              {LTD_PAYMENT_TERM},
              {ENTRY_AGE_1},
              '{GENDER_1}',
              {ENTRY_AGE_2},
              '{GENDER_2}',
              {JVA}
            )"
        ) |>
        stringr::str_remove_all(
          pattern = '[\n\r]'
        ) |>
        stringr::str_replace_all(
          pattern =
            stringr::fixed("'NULL'"),
          replacement = 
            'NULL'
        )
    ) |>
    tidyr::pivot_longer(
      cols =
        tidyselect::ends_with('SQL'),
      names_to =
        'EXTRACT_STAGE',
      names_pattern =
        '(OPENING|CLOSING)',
      values_to =
        'SQL'
    ) |>
    dplyr::semi_join(
      tibble::tribble(
        ~POLICY_FATE, ~EXTRACT_STAGE,
        'EXIT', 'OPENING',
        'REINSTATE', 'CLOSING',
        'REMAIN', 'OPENING',
        'REMAIN', 'CLOSING'
      ),
      by =
        c('POLICY_FATE', 'EXTRACT_STAGE')
    ) |>
    dplyr::summarise(
      SQL =
        paste0(SQL, collapse = ',\n')
    ) |>
    dplyr::mutate(
      SQL =
        glue::glue(
          "DELETE FROM public.ob_wol_policy_data;
          DELETE FROM public.ob_wol_extraction_headers;
          
          INSERT INTO public.ob_wol_extraction_headers(
            extraction_uid,
            extraction_date
          ) VALUES
            ('{openingExtractionUid}', '{openingExtractionDate}'),
            ('{closingExtractionUid}', '{closingExtractionDate}');
          
          INSERT INTO public.ob_wol_policy_data(
            extraction_uid,
            policy_id,
            table_code,
            status,
            sum_assured,
            entry_date,
            npdd,
            ltd_payment_term,
            entry_age_1,
            gender_1,
            entry_age_2,
            gender_2,
            joint_val_age
          ) VALUES
            {SQL};"
          
        )
    ) |>
    dplyr::pull(SQL)
  )

clipr::write_clip(
  content =
    generatedSql,
  type =
    'character'
)
  

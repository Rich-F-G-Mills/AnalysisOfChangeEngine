
library (lubridate)


params <-
  list(
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
      
      OPENING_SQL =
        glue::glue("('{openingExtractionDate}', '{openingExtractionUid}')")
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
  dplyr::count(
    OPENING_STATUS,
    CLOSING_STATUS,
    name = 'COUNT'
  )
  


library (magrittr)


telemetryEvents <-
  fs::dir_ls(
    path = "C:\\Users\\Millch\\Documents\\AnalysisOfChangeEngine\\Results Viewer\\TELEMETRY",
    regexp = '(?i)txt'
  ) |>
  unname() |>
  purrr::map(jsonlite::read_json) |>
  purrr::list_flatten() |>
  purrr::map(purrr::list_flatten, name_spec = '{inner}') |>
  purrr::map(tibble::as_tibble) |>
  purrr::map_dfr(tidyr::nest, .by = 'event_type', .key = 'data') |>
  dplyr::group_by(event_type) |>
  dplyr::group_split() |>
  purrr::map(tidyr::unnest, cols = data) |>
  purrr::map(dplyr::filter, run_uid == params$currentRunUid) |>
  purrr::map(
    dplyr::mutate,
    dplyr::across(
      tidyselect::ends_with(c('start', 'end', 'submitted')),
      lubridate::as_datetime
    )
  )

telemetryEvents <-
  telemetryEvents |>
  purrr::set_names(
    telemetryEvents |>
      purrr::map_chr(
        \(x) dplyr::first(x$event_type)
      )
  ) |>
  purrr::map(
    dplyr::select,
    -event_type
  )

formatDateTimes <-
  function (df) {
    dplyr::mutate(
      df,
      dplyr::across(
        dplyr::where(lubridate::is.POSIXct),
        \(x) format(x, "%Y-%m-%d %H:%M:%OS6")
      )
    )
  }

apiRequests <-
  telemetryEvents$api_request |>
  formatDateTimes() %T>%
  readr::write_csv(
    file = 'OUTPUTS/API_REQUESTS.CSV'
  )

dataStoreReads <-
  telemetryEvents$data_store_read |>
  formatDateTimes() %T>%
  readr::write_csv(
    file = 'OUTPUTS/DATA_STORE_READS.CSV'
  )

dataStoreWrites <-
  telemetryEvents$data_store_write |>
  formatDateTimes() %T>%
  readr::write_csv(
    file = 'OUTPUTS/DATA_STORE_WRITES.CSV'
  )

evaluationConcurrency <-
  telemetryEvents$evaluation_completed |>
  dplyr::transmute(
    run_uid,
    session_uid,
    timestamp = evaluation_start,
    change = 1L
  ) |>
  dplyr::bind_rows(
    telemetryEvents$evaluation_completed |>
      dplyr::transmute(
        run_uid,
        session_uid,
        timestamp = evaluation_end,
        change = -1L
      )
  ) |>
  dplyr::group_by(
    run_uid,
    session_uid
  ) |>
  dplyr::arrange(
    timestamp
  ) |>
  dplyr::mutate(
    num_concurrent =
      cumsum(change),
    duration =
      c(diff(timestamp), 0.0) |>
      lubridate::seconds() |>
      as.numeric()
  ) |>
  dplyr::slice_head(n = -1L) |>
  dplyr::ungroup() |>
  dplyr::select(
    num_concurrent,
    duration
  ) %T>%
  readr::write_csv(
    file = 'OUTPUTS/EVAL_CONCURRENCY.CSV'
  )
  
apiConcurrency <-
  telemetryEvents$api_request |>
  dplyr::transmute(
    run_uid,
    session_uid,
    timestamp = processing_start,
    endpoint_id,
    change = 1L
  ) |>
  dplyr::bind_rows(
    telemetryEvents$api_request |>
      dplyr::transmute(
        run_uid,
        session_uid,
        timestamp = processing_end,
        endpoint_id,
        change = -1L
      )
  ) |>
  dplyr::group_by(
    run_uid,
    session_uid,
    endpoint_id
  ) |>
  dplyr::arrange(
    timestamp
  ) |>
  dplyr::mutate(
    num_concurrent =
      cumsum(change),
    duration =
      c(diff(timestamp), 0.0) |>
      lubridate::seconds() |>
      as.numeric()
  ) |>
  dplyr::slice_head(n = -1L) |>
  dplyr::ungroup() |>
  dplyr::select(
    endpoint_id,
    num_concurrent,
    duration
  ) %T>%
  readr::write_csv(
    file = 'OUTPUTS/API_CONCURRENCY.CSV'
  )

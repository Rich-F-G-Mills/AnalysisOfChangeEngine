
library (magrittr)


telemetryEvents <-
  fs::dir_ls(
    path = "C:\\Users\\Millch\\Documents\\AnalysisOfChangeEngine\\Results Viewer\\TELEMETRY",
    regexp = '(?i)txt'
  ) |>
  purrr::map(readr::read_file) |>
  purrr::map(
    stringr::str_split,
    pattern =
      stringr::regex('(?<=\\})(?=\\{)')
  ) |>
  unlist() |>
  purrr::map(jsonlite::fromJSON) |>
  purrr::map(purrr::list_flatten, name_spec = '{inner}') |>
  purrr::map(dplyr::as_tibble) |>
  purrr::map_dfr(tidyr::nest, .by = event_type) |>
  dplyr::group_by(event_type) |>
  dplyr::group_map(
    function (df, eventType) {
      eventType <-
          as.character(eventType)
      
      df <-
        df |>
        tidyr::unnest(col = data) |>
        dplyr::filter(
          run_uid == currentRunUid
        ) |>
        dplyr::semi_join(
          stepResults,
          by = c('run_uid', 'session_uid')
        ) |>
        dplyr::mutate(
          dplyr::across(
            tidyselect::ends_with(c('_submitted', '_start', '_end')),
            lubridate::as_datetime
          )
        )
      
      tibble::lst(!!eventType := df)
    }) |>
  purrr::list_flatten()

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

completedProcessing <-
  telemetryEvents$processing_completed |>
  dplyr::left_join(
    dataStoreReads,
    by =
      c('data_store_read_idx' = 'idx', 'run_uid', 'session_uid')
  ) |>
  dplyr::left_join(
    dataStoreWrites,
    by =
      c('data_store_write_idx' = 'idx', 'run_uid', 'session_uid')
  ) |>
  formatDateTimes() %T>%
  readr::write_csv(
    file = 'OUTPUTS/COMPLETED_PROCESSING.CSV'
  )

evaluationConcurrency <-
  telemetryEvents$processing_completed |>
  dplyr::transmute(
    run_uid,
    session_uid,
    timestamp = evaluation_start,
    change = 1L
  ) |>
  dplyr::bind_rows(
    telemetryEvents$processing_completed |>
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

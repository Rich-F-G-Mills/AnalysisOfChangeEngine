
library (magrittr)


telemetryEvents <-
  readr::read_lines(
    file = 'C:\\Users\\Millch\\Desktop\\telemetry.txt'
  ) |>
  purrr::map(jsonlite::fromJSON) |>
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

telemetryEvents$api_request |>
  formatDateTimes() |>
  readr::write_csv(
    file = 'OUTPUTS/API_REQUESTS.CSV'
  )

telemetryEvents$processing_completed |>
  formatDateTimes() |>
  readr::write_csv(
    file = 'OUTPUTS/COMPLETED_PROCESSING.CSV'
  )

telemetryEvents$data_store_read |>
  formatDateTimes() |>
  readr::write_csv(
    file = 'OUTPUTS/DATA_STORE_READS.CSV'
  )

telemetryEvents$data_store_write |>
  formatDateTimes() |>
  readr::write_csv(
    file = 'OUTPUTS/DATA_STORE_WRITES.CSV'
  )





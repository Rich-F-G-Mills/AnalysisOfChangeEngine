api_request_concurrency <-
  telemetry_data$api_request |>
  dplyr::transmute(
    session_uid,
    endpoint_id,
    start = processing_start,
    end = processing_end
  ) |>
  tidyr::pivot_longer(
    cols = c(start, end),
    names_to = 'type',
    values_to = 'timestamp'
  ) |>
  dplyr::group_by(
    session_uid,
    endpoint_id
  ) |>
  dplyr::arrange(
    timestamp,
    # This will prioritise ends first.
    type
  ) |>
  dplyr::mutate(
    concurrency =
      dplyr::case_match(
        type,
        'start' ~ 1L,
        'end' ~ -1L
      ) |>
      cumsum(),
    duration =
      c(lubridate::seconds(diff(timestamp)), NA_real_)
  ) |>
  # Drop the last row within each grouping which
  # will have no duration.
  dplyr::slice_head(n = -1L) |>
  dplyr::ungroup()


api_request_concurrency |>
  dplyr::summarise(
    total_duration =
      sum(duration),
    .by = c(endpoint_id, concurrency)
  ) |>
  dplyr::mutate(
    pct =
      total_duration / sum(total_duration),
    .by = endpoint_id
  )
### Time windows
Endpoints under `/metrics` take the following query parameters to define a time window and its resolution:
- `fr`: first timestamp of time window
- `to`: last timestamp of time window
- `r`: resolution of time window, one of `block`, `1h` or `24h`

The distance between `fr` and `to` should not exceed 1000 times the resolution size, allowing for the following time windows:
- `block`: 120,000 ms * 1000 (~1.4 days, assuming 120 second blocks)
- `1h`: 3,600,000 ms * 1000 (~41 days)
- `24h`: 86,400,000 ms * 1000 (~2.7 years)

If `r` is omitted, default `block` level is used.

If none of `fr` and `to` are specified, returns last `block` level record, `r` has no effect here.

If `fr` is specified without `to`, returns records since `fr`, up to max of window size.

If `to` is specified without `fr`, returns records prior to (and including) `to`, up to max of window size.
# Terminal flush marker contract

For streaming `/v1/responses` and `/v1/responses/compact`, OAIX advertises:

```text
X-OAIX-Terminal-Flush-Marker: sse-comment-v1
```

After a `response.completed` or `response.failed` frame is successfully written
and locally flushed, OAIX writes and flushes one valid SSE comment:

```text
: oaix-terminal-flush-v1 {"schema_version":1,...}
```

The bounded JSON object contains the downstream connection ID, terminal wire
SHA-256, and the exact `flush_attempted_unix_nano` and
`flush_completed_unix_nano` values already retained in the stream-delivery
trace. It contains no request or response payload.

Marker write/flush failures are retained in stream-delivery trace schema v3 but
are observability-only: they never rewrite the outcome of an already delivered
business terminal. SSE clients may ignore comments; Ember validates and
consumes the marker without forwarding it.

# Feishu Webhook Notifier Spec (Phase 1, Outbound Only)

## Scope
- Send notifications to Feishu custom bot webhook URL
- No inbound callbacks/events
- No public endpoint needed

## Config
Environment variables:
- `FEISHU_NOTIFY_ENABLED=true|false`
- `FEISHU_WEBHOOK_URL=...`
- `FEISHU_SIGNING_SECRET=` (optional, if enabled on bot side)

## Event Types
Internal events that can trigger webhook pushes:
- `search.started`
- `search.progress` (optional throttled)
- `search.completed`
- `search.failed`
- `collection.updated`

## Payload Shape (internal)
```json
{
  "event_id": "evt_...",
  "event_type": "search.completed",
  "timestamp": "2026-03-09T04:00:00Z",
  "data": {
    "search_id": "srch_...",
    "query": "...",
    "relevant_found": 18,
    "completeness": 0.93
  }
}
```

## Delivery Rules
- Retry: exponential backoff (e.g. 1s, 2s, 4s, 8s, max 5 tries)
- Idempotency: dedupe by `event_id`
- Record failures to local dead-letter file/log

## Message Rendering
Default minimal text/card content:
- Title: status + short query
- Body: search_id, counts, completeness, elapsed time

## Security Notes
- Keep webhook URL in secret store/env only
- Never log full webhook URL in plaintext
- Validate signing if enabled

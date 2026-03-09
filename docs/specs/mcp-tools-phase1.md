# MCP Tools Spec (Phase 1)

Protocol style: MCP tools over JSON-RPC 2.0.
Transport: stdio.

## Tool List

1. `search_papers`
2. `get_search_status`
3. `get_search_results`
4. `create_collection`
5. `add_paper_to_collection`
6. `save_paper`

All tools must:
- Define `inputSchema` (JSON Schema 2020-12)
- Return structured JSON content
- Return `isError: true` for execution-level errors

---

## 1) search_papers

Purpose: start a search job.

Input schema: `schemas/search.request.schema.json`

Output shape:
```json
{
  "search_id": "srch_...",
  "status": "queued",
  "accepted_at": "2026-03-09T04:00:00Z"
}
```

---

## 2) get_search_status

Purpose: fetch progress and completeness estimate.

Input:
```json
{
  "type": "object",
  "required": ["search_id"],
  "properties": {
    "search_id": { "type": "string", "minLength": 1 }
  },
  "additionalProperties": false
}
```

Output:
```json
{
  "search_id": "srch_...",
  "status": "running",
  "progress": {
    "papers_scanned": 83,
    "relevant_found": 14
  },
  "completeness": {
    "estimate": 0.78,
    "method": "discovery_curve_v1"
  }
}
```

---

## 3) get_search_results

Purpose: list ranked results for a search job.

Input:
```json
{
  "type": "object",
  "required": ["search_id"],
  "properties": {
    "search_id": { "type": "string", "minLength": 1 },
    "cursor": { "type": "string" },
    "limit": { "type": "integer", "minimum": 1, "maximum": 100, "default": 20 }
  },
  "additionalProperties": false
}
```

Output:
```json
{
  "search_id": "srch_...",
  "items": [
    {
      "paper_id": "arxiv:2401.12345",
      "title": "...",
      "relevance": "highly_relevant",
      "score": 0.94,
      "why": "Matches all constraints including ..."
    }
  ],
  "next_cursor": "..."
}
```

---

## 4) create_collection

Input:
```json
{
  "type": "object",
  "required": ["name"],
  "properties": {
    "name": { "type": "string", "minLength": 1, "maxLength": 120 },
    "description": { "type": "string", "maxLength": 2000 }
  },
  "additionalProperties": false
}
```

Output:
```json
{
  "collection_id": "col_...",
  "name": "...",
  "created_at": "2026-03-09T04:00:00Z"
}
```

---

## 5) add_paper_to_collection

Input:
```json
{
  "type": "object",
  "required": ["collection_id", "paper_id"],
  "properties": {
    "collection_id": { "type": "string", "minLength": 1 },
    "paper_id": { "type": "string", "minLength": 1 },
    "note": { "type": "string", "maxLength": 2000 }
  },
  "additionalProperties": false
}
```

Output:
```json
{
  "collection_id": "col_...",
  "paper_id": "arxiv:...",
  "added": true
}
```

---

## 6) save_paper

Purpose: mark/save a paper to default library or specified collection.

Input:
```json
{
  "type": "object",
  "required": ["paper_id"],
  "properties": {
    "paper_id": { "type": "string", "minLength": 1 },
    "collection_id": { "type": "string" }
  },
  "additionalProperties": false
}
```

Output:
```json
{
  "paper_id": "arxiv:...",
  "saved": true,
  "collection_id": "col_..."
}
```

---

## Error Contract

MCP execution errors should return:
```json
{
  "isError": true,
  "error": {
    "code": "INVALID_ARGUMENT",
    "message": "search_id is required",
    "details": {}
  }
}
```

Error codes:
- `INVALID_ARGUMENT`
- `NOT_FOUND`
- `CONFLICT`
- `RATE_LIMITED`
- `INTERNAL`

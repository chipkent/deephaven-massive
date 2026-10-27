> For the full documentation index, see: https://massive.com/docs/llms.txt

# REST
## Indices

### Market Holidays

**Endpoint:** `GET /v1/marketstatus/upcoming`

**Description:**

Retrieve upcoming market holidays and their corresponding open/close times. This endpoint is forward-looking only, listing future holidays that affect market hours. Use this data to plan ahead for trading activities and system operations.

Use Cases: Trading schedule adjustments, integrated holiday calendars, operational planning (e.g., system maintenance), and notifying users about upcoming market closures.

## Response Attributes

| Field | Type | Description |
| --- | --- | --- |

## Sample Response

```json
[
  {
    "date": "2020-11-26",
    "exchange": "NYSE",
    "name": "Thanksgiving",
    "status": "closed"
  },
  {
    "date": "2020-11-26",
    "exchange": "NASDAQ",
    "name": "Thanksgiving",
    "status": "closed"
  },
  {
    "date": "2020-11-26",
    "exchange": "OTC",
    "name": "Thanksgiving",
    "status": "closed"
  },
  {
    "close": "2020-11-27T18:00:00.000Z",
    "date": "2020-11-27",
    "exchange": "NASDAQ",
    "name": "Thanksgiving",
    "open": "2020-11-27T14:30:00.000Z",
    "status": "early-close"
  },
  {
    "close": "2020-11-27T18:00:00.000Z",
    "date": "2020-11-27",
    "exchange": "NYSE",
    "name": "Thanksgiving",
    "open": "2020-11-27T14:30:00.000Z",
    "status": "early-close"
  }
]
```


## Plan Access

**Plan Access:** Included in all Indices plans

#### Individual Plans

| Plan | Access |
| --- | --- |
| Indices Basic | Included |
| Indices Starter | Included |
| Indices Advanced | Included |

#### Business Plans

| Plan | Access |
| --- | --- |
| Indices Business | Included |

## Plan Recency

**Plan Recency:** Updated as needed

#### Individual Plans

| Plan | Recency |
| --- | --- |
| Indices Basic | Updated as needed |
| Indices Starter | Updated as needed |
| Indices Advanced | Updated as needed |

#### Business Plans

| Plan | Recency |
| --- | --- |
| Indices Business | Updated as needed |

## Plan History

**Plan History:** Not applicable to this endpoint

> For the full documentation index, see: https://massive.com/docs/llms.txt

# REST
## Stocks

### Ticker Types

**Endpoint:** `GET /v3/reference/tickers/types`

**Description:**

Retrieve a list of all ticker types supported by Massive. This endpoint categorizes tickers across asset classes, markets, and instruments, helping users understand the different classifications and their attributes.

Use Cases: Data classification, filtering mechanisms, educational reference, system integration.

## Query Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `asset_class` | string | No | Filter by asset class. |
| `locale` | string | No | Filter by locale. |

## Response Attributes

| Field | Type | Description |
| --- | --- | --- |
| `count` | integer | The total number of results for this request. |
| `request_id` | string | A request ID assigned by the server. |
| `results` | array[object] | An array of results containing the requested data. |
| `results[].asset_class` | enum: stocks, options, crypto, fx, indices | An identifier for a group of similar financial instruments. |
| `results[].code` | string | A code used by Massive to refer to this ticker type. |
| `results[].description` | string | A short description of this ticker type. |
| `results[].locale` | enum: us, global | An identifier for a geographical location. |
| `status` | string | The status of this request's response. |


## Plan Access

**Plan Access:** Included in all Stocks plans

#### Individual Plans

| Plan | Access |
| --- | --- |
| Stocks Basic | Included |
| Stocks Starter | Included |
| Stocks Developer | Included |
| Stocks Advanced | Included |

#### Business Plans

| Plan | Access |
| --- | --- |
| Stocks Business | Included |

## Plan Recency

**Plan Recency:** Updated as needed

#### Individual Plans

| Plan | Recency |
| --- | --- |
| Stocks Basic | Updated as needed |
| Stocks Starter | Updated as needed |
| Stocks Developer | Updated as needed |
| Stocks Advanced | Updated as needed |

#### Business Plans

| Plan | Recency |
| --- | --- |
| Stocks Business | Updated as needed |

## Plan History

**Plan History:** Not applicable to this endpoint

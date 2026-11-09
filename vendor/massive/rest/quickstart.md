> For the full documentation index, see: https://massive.com/docs/llms.txt

# REST Quickstart

REST API Quickstart Guide

![A screenshot displaying a JSON response with stock data on one side and a graphical user interface presenting the same information, illustrating the types of applications you can build with Massive data](https://massive.com/docs/img/quickstart_rest_transparent_massive.png "Demonstration of JSON data powering a stocks data UI")

The Massive REST API delivers historical and real-time market data over HTTPS — ideal for on-demand queries and request/response integrations. For bulk historical downloads, use [Flat Files](https://massive.com/docs/flat-files/quickstart.md); for live streams, use the [WebSocket API](https://massive.com/docs/websocket/quickstart.md).

This guide walks through authenticating and making your first request manually so you understand what our [official client libraries](#explore-client-libraries) handle on your behalf.

## Browse Our Data

Select a category to see its REST endpoints, schemas, and code examples.

- [Stocks](https://massive.com/docs/rest/stocks/overview.md): Trades, quotes, fundamentals, and reference data.
- [Options](https://massive.com/docs/rest/options/overview.md): OPRA options trades, quotes, and aggregate bars.
- [Futures](https://massive.com/docs/rest/futures/overview.md): Trades, quotes, and reference across CME venues.
- [Indices](https://massive.com/docs/rest/indices/overview.md): Values and reference data for major U.S. indices.
- [Forex](https://massive.com/docs/rest/forex/overview.md): Currency pair trades, quotes, and aggregate bars.
- [Crypto](https://massive.com/docs/rest/crypto/overview.md): Global crypto trades, quotes, and aggregate bars.
- [Economy](https://massive.com/docs/rest/economy/overview.md): Macroeconomic indicators and historical time series.
- [Alternative](https://massive.com/docs/rest/alternative/overview.md): News, sentiment, and other alternative datasets.
- [Partners](https://massive.com/docs/rest/partners/overview.md): Third-party datasets accessible through Massive.

## Authenticate Your Request
Before you can access Massive data, you need your unique API key.

**Obtaining Your API Key**
1. [Sign up](https://massive.com/dashboard/signup) or [log in](https://massive.com/dashboard/login) to your account.
2. Visit your [Dashboard](https://massive.com/dashboard/keys) to find your API key.
3. Keep your API key secure and do not share it publicly.

You can include your API key in two ways:

* **Query String Parameter** (e.g., `?apiKey=YOUR_API_KEY`):
  **Query String Authentication**

  ```plaintext
  https://api.massive.com/v3/reference/dividends?apiKey=YOUR_API_KEY
  ```

* **Authorization Header**:
  **Header Authentication**

  ```http
  GET /v3/reference/dividends HTTP/1.1
  Host: api.massive.com
  Authorization: Bearer YOUR_API_KEY
  ```

**Note:** Replace `YOUR_API_KEY` with your actual API key from your dashboard.

## Making Your First API Request
At your terminal, let’s make a simple request using the `curl` command, to the [Dividends](https://massive.com/docs/rest/stocks/corporate-actions/dividends.md) endpoint to list recent dividends.

**cURL**

```bash
curl "https://api.massive.com/v3/reference/dividends?apiKey=YOUR_API_KEY"
```

This command retrieves dividend records. For more parameters and advanced filtering options, see the Dividends endpoint [documentation](https://massive.com/docs/rest/stocks/corporate-actions/dividends.md).

**Pro Tip:** If you omit your API key or use an invalid one, you will receive an authorization error. Ensure you have replaced `YOUR_API_KEY` with your actual key before running the command.

## Understanding the API Response
All REST API endpoints from Massive return data in a structured JSON format. You will commonly see fields like `status`, `count`, `results`, and `request_id` at the root level. The `results` field typically contains an array of objects, each representing an individual data record (such as a dividend, trade, or ticker detail).

**JSON Response**

```json
{
  "results": [
      {
          "cash_amount": 0.25,
          "currency": "USD",
          "declaration_date": "2024-10-31",
          "dividend_type": "CD",
          "ex_dividend_date": "2024-11-08",
          "frequency": 4,
          "id": "E416a068758f85277196150c3eb73a3331d04698856c141e883ad95710dd0b189",
          "pay_date": "2024-11-14",
          "record_date": "2024-11-11",
          "ticker": "AAPL"
      }
  ],
  "status": "OK",
  "request_id": "5a8e1e551dc3a1c2c203744543b40399",
}
```

This pattern is consistent across our REST endpoints. Once you understand how to authenticate, make requests, and parse JSON responses for one endpoint -- like Dividends -- you can apply the same approach to all other endpoints. As you explore different datasets (e.g., trades, quotes, fundamentals), you will find that the process remains the same: use your API key, structure your request URL or headers appropriately, and parse the returned JSON to integrate the data into your applications.

Now that you know how to authenticate and issue requests, you have the basic skills to use any of the Massive REST endpoints.

## Apache Arrow Responses

Some endpoints can also return the same data as an [Apache Arrow](https://arrow.apache.org/) file instead of JSON, a compact binary columnar format that is faster to transfer and parse for large result sets. To request it, send an `Accept: application/vnd.apache.arrow.file` header. Where an endpoint supports this, its reference page shows a JSON/Arrow toggle.

The response is a binary Arrow file, not text, so decode it with an Arrow library for your language rather than a JSON parser. The [Apache Arrow libraries](https://arrow.apache.org/install/) are available for most languages and return a table you can iterate row by row or read column by column, then use the same way you would the JSON `results`.

To paginate, note that the binary body does not carry the `next_url` field that a JSON response includes. Instead, the next page's URL is returned in the `Link` response header with `rel="next"` (for example, `Link: <https://api.massive.com/fed/v1/treasury-yields?cursor=...>; rel="next"`). Read that header, request the URL for the next page, and stop once no `rel="next"` link is returned.

## Explore Client Libraries
While understanding the manual workflow is valuable, integrating directly with our client libraries is the recommended approach for production use. These libraries handle:

* Authentication and secure key management
* Request formatting and retry logic
* Parsing JSON responses into easy-to-use structures

Available libraries:
* **Python**: [GitHub - Python Client](https://github.com/massive-com/client-python)
* **Go**: [GitHub - Go Client](https://github.com/massive-com/client-go)
* **Kotlin**: [GitHub - JVM Client](https://github.com/massive-com/client-jvm)
* **JavaScript**: [GitHub - JavaScript Client](https://github.com/massive-com/client-js)

## Using Massive with AI

Connecting an AI agent or assistant to Massive? Our [AI Tools](https://massive.com/docs/ai-tools/quickstart.md) area covers everything you need to give LLMs reliable access to market data: a remote MCP server for the REST API, LLM-friendly `llms.txt` files, and Markdown versions of every documentation page (just append `.md` to any URL). It includes step-by-step setup for Claude, ChatGPT, Cursor, Codex, and more.

## Next Steps
With your first successful API request and an understanding of the common patterns in our JSON responses, you are prepared to explore any of Massive’s endpoints. Check out our detailed documentation for additional parameters, data sources, and capabilities. As you progress, consider using our client libraries to greatly simplify the integration process and focus on building data-driven applications.

> For the full documentation index, see: https://massive.com/docs/llms.txt

# Flat Files Quickstart

Flat Files Quickstart Guide

![A screenshot displaying CSV data and a graphical user interface powered by Flat Files, illustrating streamlined market data integration](https://massive.com/docs/img/quickstart_flatfiles_transparent.png "Example of CSV Flat File integration")

Massive's Flat Files deliver extensive historical market data as compressed CSVs over an S3-compatible endpoint — ideal when you need bulk historical data without making thousands of REST calls. For smaller on-demand queries, use the [REST API](https://massive.com/docs/rest/quickstart.md); for real-time streams, use the [WebSocket API](https://massive.com/docs/websocket/quickstart.md).

This guide walks through downloading datasets manually with popular S3 clients or our File Browser.

## Browse Our Data

Select a category to see what data is available as Flat Files.

- [Stocks](https://massive.com/docs/flat-files/stocks/overview.md): Trades, quotes, and aggregate bars for U.S. equities.
- [Options](https://massive.com/docs/flat-files/options/overview.md): OPRA options trades, quotes, and aggregate bars.
- [Futures](https://massive.com/docs/flat-files/futures/overview.md): Trades, quotes, and aggregates across CME venues.
- [Indices](https://massive.com/docs/flat-files/indices/overview.md): Values and aggregates from major index providers.
- [Forex](https://massive.com/docs/flat-files/forex/overview.md): Currency pair quotes and time-based aggregate bars.
- [Crypto](https://massive.com/docs/flat-files/crypto/overview.md): Global crypto trades, quotes, and aggregate bars.

## Exploring and Downloading Data

Flat Files are organized by asset class and data type. Available asset classes include Stocks, Options, Indices, Forex, and Crypto. Within each asset class, you'll find data types such as trades, quotes, minute aggregates, day aggregates, and more.

To quickly explore and download historical data files, use the File Browser located in the left-hand navigation of this page. Simply select the asset class you're interested in and then drill down to access specific datasets available with your subscription. Alternatively, you can use an S3 client to automate and integrate Flat Files data into your workflow, as described in the following sections.

Note: Data for each trading day is available by approximately 11:00 AM ET the following day.

### Sample Data

Here's an example of minute aggregates for the stock `AAPL`, showing the CSV structure you'll see when downloading Flat Files:

```csv
ticker,volume,open,close,high,low,window_start,transactions
AAPL,4930,200.29,200.5,200.63,200.29,1744792500000000000,129
AAPL,1815,200.39,200.34,200.61,200.34,1744792560000000000,57
AAPL,1099,200.3,200.28,200.3,200.13,1744792620000000000,40
AAPL,3672,200.39,200.61,200.64,200.39,1744792680000000000,71
AAPL,4322,200.72,200.69,200.8,200.69,1744792740000000000,88
AAPL,3675,200.7,201.5,201.5,200.7,1744792800000000000,119
AAPL,12785,201.49,202.33,202.33,201.49,1744792860000000000,329
AAPL,11473,202.39,201.81,202.46,201.81,1744792920000000000,199
AAPL,3895,202.0,201.82,202.0,201.65,1744792980000000000,116
AAPL,4322,201.76,201.36,201.76,201.17,1744793040000000000,85
AAPL,2089,201.31,201.35,201.35,201.04,1744793100000000000,48
AAPL,7317,201.31,200.88,201.31,200.71,1744793160000000000,121
```

Each CSV file you download includes a header line as the first row, clearly identifying each column. This header exists across all files within a specific dataset, making it easy for you to interpret and integrate the data into your workflows.

## Setting Up S3 Access

To download Flat Files using an S3 client, you'll first need to configure your environment with the appropriate access credentials provided by Massive:

- Ensure you have an active [Massive subscription](https://massive.com/pricing) with Flat Files access.
- Obtain your unique S3 Access Key and Secret Key from your [Dashboard](https://massive.com/dashboard/keys).

Use the following standard configuration details for all S3 clients:

- **Endpoint:** `https://files.massive.com`
- **Bucket Name:** `flatfiles`

Massive officially supports several popular S3-compatible clients:

- **[AWS S3 CLI](#aws-s3-cli)**
- **[Rclone](#rclone)**
- **[MinIO](#minio)**
- **[Python Boto3 SDK](#python-boto3-sdk)**

Below you'll find quick setup instructions for each client.

## AWS S3 CLI
The AWS S3 CLI is an efficient command-line tool for managing S3-compatible data stores.

Here's how to get started:

* Install the AWS S3 CLI from the [official website](https://aws.amazon.com/cli/).
* Run `aws configure` in your command line and enter your Access Key ID, and Secret Access Key.
* To interact with Massive's S3 files, use aws s3 commands:

  **AWS S3 CLI**

  ```bash
  # Configure your S3 Access and Secret keys
  aws configure set aws_access_key_id YOUR_ACCESS_KEY_ID
  aws configure set aws_secret_access_key YOUR_SECRET_ACCESS_KEY

  # List
  aws s3 ls s3://flatfiles/ --endpoint-url https://files.massive.com

  # Copy
  aws s3 cp s3://flatfiles/us_stocks_sip/trades_v1/2025/11/2025-11-05.csv.gz . --endpoint-url https://files.massive.com
  ```

## Rclone
Rclone is versatile for syncing and managing S3-compatible storage directly from your command line.

Here's how to get started:

* Download and install Rclone from [here](https://rclone.org/downloads/).
* Initiate a new remote storage configuration with `rclone config`.
* Here's an example configuration is provided below for reference:

  **Rclone**

  ```bash
  # Set up your rclone configuration
  rclone config create s3massive s3 env_auth=false access_key_id=YOUR_ACCESS_KEY_ID secret_access_key=YOUR_SECRET_ACCESS_KEY endpoint=https://files.massive.com

  # List
  rclone ls s3massive:flatfiles

  # Copy
  rclone copy s3massive:flatfiles/us_stocks_sip/trades_v1/2025/11/2025-11-05.csv.gz .
  ```

## MinIO
MinIO client works seamlessly with any S3 compatible cloud storage.

Here's how to get started:

* If you haven't already, download and set up MinIO from [their documentation](https://docs.min.io/docs/minio-client-quickstart-guide).
* Here's an example configuration is provided below for reference:

  **MinIO**

  ```bash
  # Enter S3 Access and Secret keys in config file ~/.mc/config.json
  mc alias set s3massive https://files.massive.com YOUR_ACCESS_KEY_ID YOUR_SECRET_ACCESS_KEY

  # List
  mc ls s3massive/flatfiles

  # View
  mc cat s3massive/flatfiles/us_stocks_sip/trades_v1/2025/11/2025-11-05.csv.gz | gzcat | head -4

  # Copy
  mc cp s3massive/flatfiles/us_stocks_sip/trades_v1/2025/11/2025-11-05.csv.gz .
  ```

## Python Boto3 SDK
Boto3 is the Amazon Web Services (AWS) SDK for Python, which enables Python developers to write software that makes use of Amazon services like S3.

Here's how to get started:

* Install Boto3 by running `pip install boto3` if you haven't already.
* Utilize the following script to interact with Massive data:

  **Python Boto3 SDK**

  ```python
  import boto3
  from botocore.config import Config

  # Initialize a session using your credentials
  session = boto3.Session(
    aws_access_key_id='YOUR_ACCESS_KEY_ID',
    aws_secret_access_key='YOUR_SECRET_ACCESS_KEY',
  )

  # Create a client with your session and specify the endpoint
  s3 = session.client(
    's3',
    endpoint_url='https://files.massive.com',
    config=Config(signature_version='s3v4'),
  )

  # List Example
  # Initialize a paginator for listing objects
  paginator = s3.get_paginator('list_objects_v2')

  # Choose the appropriate prefix depending on the data you need:
  # - 'global_crypto' for global cryptocurrency data
  # - 'global_forex' for global forex data
  # - 'us_indices' for US indices data
  # - 'us_options_opra' for US options (OPRA) data
  # - 'us_stocks_sip' for US stocks (SIP) data
  prefix = 'us_stocks_sip'  # Example: Change this prefix to match your data need

  # List objects using the selected prefix
  for page in paginator.paginate(Bucket='flatfiles', Prefix=prefix):
    for obj in page['Contents']:
      print(obj['Key'])

  # Copy example
  # Specify the bucket name
  bucket_name = 'flatfiles'

  # Specify the S3 object key name
  object_key = 'us_stocks_sip/trades_v1/2025/11/2025-11-05.csv.gz'

  # Specify the local file name and path to save the downloaded file
  # This splits the object_key string by '/' and takes the last segment as the file name
  local_file_name = object_key.split('/')[-1]

  # This constructs the full local file path
  local_file_path = './' + local_file_name

  # Download the file
  s3.download_file(bucket_name, object_key, local_file_path)
  ```

## Using Massive with AI

Connecting an AI agent or assistant to Massive? Our [AI Tools](https://massive.com/docs/ai-tools/quickstart.md) area covers everything you need to give LLMs reliable access to market data: a remote MCP server for the REST API, LLM-friendly `llms.txt` files, and Markdown versions of every documentation page (just append `.md` to any URL). It includes step-by-step setup for Claude, ChatGPT, Cursor, Codex, and more.

## Next Steps
Now that you've successfully accessed Flat Files with your S3 client, you can easily automate data retrieval and integrate historical market data into your workflows.

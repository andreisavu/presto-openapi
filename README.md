# presto-openapi
The OpenAPI HTTP/JSON alternative to the [Thrift Presto connector](https://prestodb.io/docs/current/connector/thrift.html#connector-thrift--page-root). Follow this [OpenAPI Specification](https://editor.swagger.io/?url=https://raw.githubusercontent.com/andreisavu/presto-openapi/main/openapi.yaml) if you want to implement a compatible API endpoint. See `python-api-example/api.py` for a simple reference implementation that serves a bunch of CSV files.

**Warning**: This is a proof of concept and should not be used in production yet. The implementation is under active development and the API is subject to change.

## Limitations

### Supported data types

| Presto Type | OpenAPI Type |
|-------------|--------------|
| varchar     | string       |

### Supported push downs

Equality filters on varchar columns are pushed down to the API.

## Quick start

### Building the plugin

This requires JDK 11. I've used the [Amazon Corretto](https://aws.amazon.com/corretto/?filtered-posts.sort-by=item.additionalFields.createdDate&filtered-posts.sort-order=desc) distribution.

    ./mvnw clean package -DskipTests

All the necessary binaries will be placed at `target/presto-openapi-0.286`.

### Installing the plugin

To install it all you have to do is copy the folder above under the `plugin/presto-openapi` in your Presto distribution of choice. The plugin was compiled against the Presto 0.286 version of the SPI.

### Configuring the plugin

Add a new catalog file and set the base URL:

    $ cat etc/catalog/example.properties
    connector.name=presto-openapi
    presto-openapi.base_url=http://localhost:8080
    presto-openapi.auth.bearer_token=your_hardcoded_token

Use the Python example implementation for a test:

    $ cd python-example-api
    $ poetry install
    $ poetry run python api.py
    Bottle v0.12.25 server starting up (using WSGIRefServer())...
    Listening on http://localhost:8080/
    Hit Ctrl-C to quit.

### Configuration options

| Option                                          | Description                                                 | Default |
|-------------------------------------------------|-------------------------------------------------------------|---------|
| `presto-openapi.base_url`                       | The base URL of the OpenAPI endpoint                        |         |
| `presto-openapi.auth.bearer_token`              | The bearer token to use for authentication                  |         |
| `presto-openapi.auth.basic.username`            | The basic auth username to use for authentication           |         |
| `presto-openapi.auth.basic.password`            | The basic auth password to use for authentication           |         |
| `presto-openapi.auth.api_key`                   | The API key to use for authentication (as X-Presto-API-Key) |         |
| `presto-openapi.metadata_refresh_threads`       | The number of threads to use for refreshing metadata        | 1       |
| `presto-openapi.metadata_refresh_interval_ms`   | The interval at which to refresh table metadata             | 60000   |
| `presto-openapi.http-client.connect_timeout_ms` | The connection timeout in milliseconds                      | 10000   |
| `presto-openapi.http-client.read_timeout_ms`    | The read timeout in milliseconds                            | 10000   |
| `presto-openapi.http-client.write_timeout_ms`   | The write timeout in milliseconds                           | 10000   |

### Running queries

#### Show the schemas available for the new catalog:

    presto> show schemas from example;
    Schema
    --------------------
    information_schema
    inventory          
    sales              
    (3 rows)
    
    Query 20240414_172206_00002_2dpjs, FINISHED, 1 node
    Splits: 19 total, 19 done (100.00%)
    [Latency: client-side: 261ms, server-side: 251ms] [3 rows, 47B] [11 rows/s, 187B/s]

In the background you will see HTTP API requests being made to the Python server.

#### Show the tables for a schema

    presto> show tables from example.sales;
    Table
    -----------
    customers
    orders    
    (2 rows)
    
    Query 20240414_194324_00016_yh9jx, FINISHED, 1 node
    Splits: 19 total, 19 done (100.00%)
    [Latency: client-side: 103ms, server-side: 97ms] [2 rows, 45B] [20 rows/s, 463B/s]

#### Show the columns for a table

    presto> show columns from example.sales.orders;
    Column    |  Type   | Extra | Comment
    --------------+---------+-------+---------
    order_id     | varchar |       |         
    customer_id  | varchar |       |         
    order_date   | varchar |       |         
    total_amount | varchar |       |         
    (4 rows)
    
    Query 20240414_212512_00000_j92py, FINISHED, 1 node
    Splits: 19 total, 19 done (100.00%)
    [Latency: client-side: 0:01, server-side: 0:01] [8 rows, 534B] [9 rows/s, 644B/s]

This will trigger a refresh for the table metadata and store in the process cache.

#### Select all columns, filter and order

    presto> select * from example.sales.orders where customer_id = '1' order by order_id;
    order_id | customer_id | order_date | total_amount
    ----------+-------------+------------+--------------
    1        | 1           | 2023-01-01 | 100.50       
    12       | 1           | 2023-01-12 | 95.00        
    15       | 1           | 2023-01-15 | 160.00       
    18       | 1           | 2023-01-18 | 100.00       
    21       | 1           | 2023-01-21 | 180.00       
    24       | 1           | 2023-01-24 | 95.00        
    27       | 1           | 2023-01-27 | 170.00       
    3        | 1           | 2023-01-03 | 120.00       
    30       | 1           | 2023-01-30 | 100.00       
    6        | 1           | 2023-01-06 | 50.00        
    9        | 1           | 2023-01-09 | 175.50       
    (11 rows)
    
    Query 20240415_044421_00009_eewnp, FINISHED, 1 node
    Splits: 48 total, 48 done (100.00%)
    [Latency: client-side: 0:03, server-side: 0:03] [30 rows, 1.12KB] [9 rows/s, 357B/s]

In background, this will request multiple splits, and for each split multiple pages.

#### Calling a Python lambda function

In `api.py` there is a definition that looks like this:

    FUNCTIONS_SCHEMA_NAME = 'virtual'
    FUNCTIONS = {
    'permutations': lambda *, word: [''.join(p) for p in permutations(word)]
    }

It can be queried like this:

    presto> select result from example.virtual.permutations where word='rocket!!!' order by rand() limit 10;
    result
    -----------
    !ro!c!tke
    rkc!!o!et
    !ktcr!o!e
    !tc!!kreo
    ce!!okt!r
    korc!!!te
    k!ctor!!e
    e!kortc!!
    tec!!r!ko
    eort!!!ck
    (10 rows)
    
    Query 20240417_235528_00009_vt9yf, FINISHED, 1 node
    Splits: 18 total, 18 done (100.00%)
    [Latency: client-side: 436ms, server-side: 423ms] [363K rows, 9.69MB] [858K rows/s, 22.9MB/s]

The value of `word` as a column filter is passed as a parameter to the lambda function.

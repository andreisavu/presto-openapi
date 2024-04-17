import os
import csv
import json
import base64
import logging

from bottle import Bottle, route, run, response, request
from bottle_cors_plugin import cors_plugin

app = Bottle()
app.install(cors_plugin('*'))

CSV_DIRECTORY = 'data'
MAX_SPLIT_SIZE = 5

logging.basicConfig(level=logging.INFO)

def read_csv_file(file_path):
    with open(file_path, 'r') as file:
        reader = csv.reader(file)
        header = next(reader)
        data = list(reader)
    return header, data

@app.route('/schemas', method='GET')
def list_schemas():
    schemas = [name for name in os.listdir(CSV_DIRECTORY) if os.path.isdir(os.path.join(CSV_DIRECTORY, name))]
    response.content_type = 'application/json'
    return json.dumps(schemas)

@app.route('/schemas/<schema>/tables', method='GET')
def list_tables(schema):
    schema_path = os.path.join(CSV_DIRECTORY, schema)
    tables = [{'schema': schema, 'table': name.split('.')[0]} for name in os.listdir(schema_path) if name.endswith('.csv')]
    response.content_type = 'application/json'
    return json.dumps(tables)

@app.route('/schemas/<schema>/tables/<table>', method='GET')
def get_table_metadata(schema, table):
    file_path = os.path.join(CSV_DIRECTORY, schema, f'{table}.csv')
    header, _ = read_csv_file(file_path)
    columns = [{'name': column, 'type': 'varchar', 'comment': None, 'hidden': False} for column in header]
    metadata = {
        'schemaTableName': {'schema': schema, 'table': table},
        'columns': columns,
        'comment': None,
    }
    response.content_type = 'application/json'
    return json.dumps(metadata)

@app.route('/schemas/<schema>/tables/<table>/splits', method='POST')
def get_splits(schema, table):
    logging.info(f'Splits request: {request.json}')

    file_path = os.path.join(CSV_DIRECTORY, schema, f'{table}.csv')
    _, data = read_csv_file(file_path)
    max_split_size = min(MAX_SPLIT_SIZE, len(data))

    splits = []
    for i in range(0, len(data), max_split_size):
        start = i
        end = min(start + max_split_size, len(data))
        splits.append(f"{start}-{end}")

    split_batch = {'splits': splits}
    response.content_type = 'application/json'
    return json.dumps(split_batch)

@app.route('/schemas/<schema>/tables/<table>/splits/<split_id>/rows', method='POST')
def get_rows(schema, table, split_id):
    logging.info(f'Rows request: {request.json}')

    file_path = os.path.join(CSV_DIRECTORY, schema, f'{table}.csv')
    header, data = read_csv_file(file_path)

    desiredColumns = request.json.get('desiredColumns', None)
    if desiredColumns is not None:
        column_indices = [header.index(column) for column in desiredColumns]
    else:
        # Return all columns in the order they are set in the file
        column_indices = list(range(len(header)))

    next_token = request.json.get('nextToken')
    if next_token:
        start, end = map(int, next_token.split('-'))
    else:
        start, end = map(int, split_id.split('-'))

    if next_token:
        rows = data[start:end]
        next_token = None
    else:
        mid = (start + end) // 2
        rows = data[start:mid]
        next_token = f"{mid}-{end}"

    column_blocks = []
    for i in column_indices:
        column_data = [row[i] for row in rows]
        column_blocks.append({
            'varcharData': {
                'nulls': [False] * len(column_data),
                'sizes': [len(value) for value in column_data],
                'bytes': base64.b64encode(''.join(column_data).encode('utf-8')).decode('utf-8')
            }
        })

    page_result = {'columnBlocks': column_blocks, 'rowCount': len(rows), 'nextToken': next_token}
    response.content_type = 'application/json'
    return json.dumps(page_result)

if __name__ == '__main__':
    run(app, host='localhost', port=8080)
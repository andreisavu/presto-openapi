import os
import csv
import json
import base64
import logging

from itertools import permutations
from inspect import signature

from bottle import Bottle, route, run, response, request
from bottle_cors_plugin import cors_plugin

app = Bottle()
app.install(cors_plugin('*'))

CSV_DIRECTORY = 'data'
MAX_SPLIT_SIZE = 5

FUNCTIONS_SCHEMA_NAME = 'virtual'
FUNCTIONS = {
    'permutations': lambda *, word: [''.join(p) for p in permutations(word)]
}

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
    schemas.append(FUNCTIONS_SCHEMA_NAME) # tables mapping back into function calls
    response.content_type = 'application/json'
    return json.dumps(schemas)

@app.route('/schemas/<schema>/tables', method='GET')
def list_tables(schema):
    if schema == FUNCTIONS_SCHEMA_NAME:
        response.content_type = 'application/json'
        tables = []
        for function_name in FUNCTIONS:
            tables.append({'schema': schema, 'table': function_name})
        return json.dumps(tables)
    else:
        schema_path = os.path.join(CSV_DIRECTORY, schema)
        tables = [{'schema': schema, 'table': name.split('.')[0]} for name in os.listdir(schema_path) if name.endswith('.csv')]
        response.content_type = 'application/json'
        return json.dumps(tables)

@app.route('/schemas/<schema>/tables/<table>', method='GET')
def get_table_metadata(schema, table):
    if schema == FUNCTIONS_SCHEMA_NAME:
        function = FUNCTIONS.get(table)
        if function is None:
            response.status = 404
            return

        signature_params = list(signature(function).parameters.keys())
        columns = [{'name': param, 'type': 'varchar'} for param in signature_params]
        columns.append({'name': 'result', 'type': 'varchar'})
        metadata = {
            'schemaTableName': {'schema': schema, 'table': table},
            'columns': columns,
            'comment': None,
        }
    else:
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

    # All virtual tables have only one split
    if schema == FUNCTIONS_SCHEMA_NAME:
        response.content_type = 'application/json'
        return json.dumps({'splits': ['0']})

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

def construct_column_block(column_data):
    return {
        'varcharData': {
            'nulls': [False] * len(column_data),
            'sizes': [len(value) for value in column_data],
            'bytes': base64.b64encode(''.join(column_data).encode('utf-8')).decode('utf-8')
        }
    }

def get_column_indices(header, desired_columns):
    if desired_columns is not None:
        return [header.index(column) for column in desired_columns]
    else:
        return list(range(len(header)))

def get_rows_range(split_id, next_token, data):
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

    return rows, next_token

def handle_function_call(table, request_json):
    function = FUNCTIONS.get(table)
    if function is None:
        response.status = 404
        return

    method_kwargs_names = list(signature(function).parameters.keys())
    desired_columns = request_json.get('desiredColumns')
    assert desired_columns == method_kwargs_names + ['result']

    # Extract the method parameters from the input
    method_kwargs = {}
    for param in method_kwargs_names:
        param_data = request_json['outputConstraint']['domains'][param]['valueSet']['equatable']['values'][0]['varcharData']
        param_value = base64.b64decode(param_data['bytes']).decode('utf-8')
        method_kwargs[param] = param_value

    # Call the function with the extracted parameters
    result = function(**method_kwargs)
    row_count = len(result)

    column_blocks = []
    for column in desired_columns:
        if column in method_kwargs_names:
            column_data = [method_kwargs[column]] * row_count
        elif column == 'result':
            column_data = result
        else:
            raise ValueError(f'Unknown column: {column}')
        column_block = construct_column_block(column_data)
        column_blocks.append(column_block)

    response.content_type = 'application/json'
    return json.dumps({'columnBlocks': column_blocks, 'rowCount': row_count})

@app.route('/schemas/<schema>/tables/<table>/splits/<split_id>/rows', method='POST')
def get_rows(schema, table, split_id):
    logging.info(f'Rows request: {request.json}')

    if schema == FUNCTIONS_SCHEMA_NAME:
        result = handle_function_call(table, request.json)
        if result is not None:
            return result
        else:
            return

    file_path = os.path.join(CSV_DIRECTORY, schema, f'{table}.csv')
    header, data = read_csv_file(file_path)

    desired_columns = request.json.get('desiredColumns', None)
    column_indices = get_column_indices(header, desired_columns)

    next_token = request.json.get('nextToken')
    rows, next_token = get_rows_range(split_id, next_token, data)

    column_blocks = []
    for i in column_indices:
        column_data = [row[i] for row in rows]
        column_block = construct_column_block(column_data)
        column_blocks.append(column_block)

    page_result = {'columnBlocks': column_blocks, 'rowCount': len(rows), 'nextToken': next_token}
    response.content_type = 'application/json'
    return json.dumps(page_result)

if __name__ == '__main__':
    run(app, host='localhost', port=8080)
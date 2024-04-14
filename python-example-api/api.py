import os
import csv
import json
from bottle import Bottle, route, run, response, request

app = Bottle()

CSV_DIRECTORY = 'data'

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
    columns = [{'name': column, 'type': 'string', 'comment': None, 'hidden': False} for column in header]
    metadata = {
        'schemaTableName': {'schema': schema, 'table': table},
        'columns': columns,
        'comment': None,
        'indexableKeys': []
    }
    response.content_type = 'application/json'
    return json.dumps(metadata)

@app.route('/schemas/<schema>/tables/<table>/splits', method='POST')
def get_splits(schema, table):
    file_path = os.path.join(CSV_DIRECTORY, schema, f'{table}.csv')
    _, data = read_csv_file(file_path)
    max_split_count = request.json.get('maxSplitCount', len(data))
    next_token = request.json.get('nextToken', None)

    if next_token is not None:
        start = int(next_token)
    else:
        start = 0

    end = min(start + max_split_count, len(data))

    splits = []
    for i in range(start, end):
        split_id = f"{schema}_{table}_{i}"
        splits.append({
            'splitId': split_id,
            'hosts': [{'host': 'localhost', 'port': 8080}]
        })

    if end < len(data):
        next_token = str(end)
    else:
        next_token = None

    split_batch = {'splits': splits, 'nextToken': next_token}
    response.content_type = 'application/json'
    return json.dumps(split_batch)

@app.route('/splits/<split_id>/rows', method='POST')
def get_rows(split_id):
    schema, table, row_index = split_id.split('_')
    file_path = os.path.join(CSV_DIRECTORY, schema, f'{table}.csv')
    header, data = read_csv_file(file_path)

    start = int(row_index)
    end = start + 1

    rows = data[start:end]

    column_blocks = []
    for i in range(len(header)):
        column_data = [row[i] for row in rows]
        column_blocks.append({
            'varcharData': {
                'nulls': [False] * len(column_data),
                'sizes': [len(value) for value in column_data],
                'bytes': ''.join(column_data).encode('utf-8').hex()
            }
        })

    page_result = {'columnBlocks': column_blocks, 'rowCount': len(rows), 'nextToken': None}
    response.content_type = 'application/json'
    return json.dumps(page_result)

if __name__ == '__main__':
    run(app, host='localhost', port=8080)
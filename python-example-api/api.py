import os
import csv
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
    return {'schemas': schemas}

@app.route('/schemas/<schema>/tables', method='GET')
def list_tables(schema):
    schema_path = os.path.join(CSV_DIRECTORY, schema)
    tables = [name.split('.')[0] for name in os.listdir(schema_path) if name.endswith('.csv')]
    response.content_type = 'application/json'
    return {'tables': tables}

@app.route('/schemas/<schema>/tables/<table>', method='GET')
def get_table_metadata(schema, table):
    file_path = os.path.join(CSV_DIRECTORY, schema, f'{table}.csv')
    header, _ = read_csv_file(file_path)
    columns = [{'name': column, 'type': 'string'} for column in header]
    response.content_type = 'application/json'
    return {'schema': schema, 'table': table, 'columns': columns}

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
        splits.append({
            'splitId': str(i),
            'hosts': [{'host': 'localhost', 'port': 8080}],
            'start': i,
            'end': i + 1
        })
    if end < len(data):
        next_token = str(end)
    else:
        next_token = None
    response.content_type = 'application/json'
    return {'splits': splits, 'nextToken': next_token}

@app.route('/splits/<split_id>/rows', method='POST')
def get_rows(split_id):
    schema = request.query.get('schema')
    table = request.query.get('table')
    file_path = os.path.join(CSV_DIRECTORY, schema, f'{table}.csv')
    header, data = read_csv_file(file_path)
    start = int(request.json['start'])
    end = int(request.json['end'])
    rows = data[start:end]
    column_blocks = [[row[i] for row in rows] for i in range(len(header))]
    response.content_type = 'application/json'
    return {'columnBlocks': column_blocks, 'rowCount': len(rows), 'nextToken': None}

if __name__ == '__main__':
    run(app, host='localhost', port=8080)
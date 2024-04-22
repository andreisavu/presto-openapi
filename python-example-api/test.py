import base64
import requests

BASE_URL = 'http://localhost:8080'

HEADERS_WITH_BEARER_TOKEN = {
    'Content-Type': 'application/json',
    'Authorization': 'Bearer your_hardcoded_token',
}

HEADERS_WITH_BASIC_AUTH = {
    'Content-Type': 'application/json',
    'Authorization': 'Basic ' + base64.b64encode(b'test_username:test_password').decode('utf-8'),
}

def test_list_schemas():
    response = requests.get(f'{BASE_URL}/schemas', headers=HEADERS_WITH_BASIC_AUTH)
    assert response.status_code == 200
    assert response.json() == ['sales', 'inventory', 'virtual']

def test_list_tables():
    response = requests.get(f'{BASE_URL}/schemas/sales/tables', headers=HEADERS_WITH_BASIC_AUTH)
    assert response.status_code == 200
    expected_tables = [
        {'schema': 'sales', 'table': 'orders'},
        {'schema': 'sales', 'table': 'customers'}
    ]
    assert sorted(response.json(), key=lambda x: x['table']) \
           == sorted(expected_tables, key=lambda x: x['table'])

def test_get_table_metadata():
    response = requests.get(f'{BASE_URL}/schemas/sales/tables/orders', headers=HEADERS_WITH_BEARER_TOKEN)
    assert response.status_code == 200
    expected_metadata = {
        'schemaTableName': {'schema': 'sales', 'table': 'orders'},
        'columns': [
            {'name': 'order_id', 'type': 'varchar', 'comment': None, 'hidden': False},
            {'name': 'customer_id', 'type': 'varchar', 'comment': None, 'hidden': False},
            {'name': 'order_date', 'type': 'varchar', 'comment': None, 'hidden': False},
            {'name': 'total_amount', 'type': 'varchar', 'comment': None, 'hidden': False}
        ],
        'comment': None,
    }
    assert response.json() == expected_metadata

def test_get_splits_and_rows():
    max_split_count = 50
    next_token = None
    schema = 'sales'
    table = 'orders'

    # Get a batch of splits
    response = requests.post(f'{BASE_URL}/schemas/{schema}/tables/{table}/splits',
                             json={'maxSplitCount': max_split_count}, headers=HEADERS_WITH_BEARER_TOKEN)
    assert response.status_code == 200
    splits = response.json()['splits']

    # Test each split in the batch
    for split in splits:
        row_count = 0
        next_token = None
        while True:
            response = requests.post(f"{BASE_URL}/schemas/{schema}/tables/{table}/splits/{split}/rows",
                                     json={'nextToken': next_token}, headers=HEADERS_WITH_BEARER_TOKEN)
            assert response.status_code == 200
            row_data = response.json()
            row_count += row_data['rowCount']
            if not row_data['nextToken']:
                break
            next_token = row_data['nextToken']
        assert row_count == 5, f'Expected 5 rows, got {row_count}'

    # Check if all splits cover the entire dataset
    assert len(splits) == 6
    for split in splits:
        start, end = map(int, split.split('-'))
        assert start >= 0 and end >= start

if __name__ == '__main__':
    test_list_schemas()
    test_list_tables()
    test_get_table_metadata()
    test_get_splits_and_rows()
    print('All tests passed!')
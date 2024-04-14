import requests

BASE_URL = 'http://localhost:8080'

def test_list_schemas():
    response = requests.get(f'{BASE_URL}/schemas')
    assert response.status_code == 200
    assert response.json() == ['sales', 'inventory']

def test_list_tables():
    response = requests.get(f'{BASE_URL}/schemas/sales/tables')
    assert response.status_code == 200
    assert response.json() == [
        {'schema': 'sales', 'table': 'orders'},
        {'schema': 'sales', 'table': 'customers'}
    ]

def test_get_table_metadata():
    response = requests.get(f'{BASE_URL}/schemas/sales/tables/orders')
    assert response.status_code == 200
    expected_metadata = {
        'schemaTableName': {'schema': 'sales', 'table': 'orders'},
        'columns': [
            {'name': 'order_id', 'type': 'string', 'comment': None, 'hidden': False},
            {'name': 'customer_id', 'type': 'string', 'comment': None, 'hidden': False},
            {'name': 'order_date', 'type': 'string', 'comment': None, 'hidden': False},
            {'name': 'total_amount', 'type': 'string', 'comment': None, 'hidden': False}
        ],
        'comment': None,
        'indexableKeys': []
    }
    assert response.json() == expected_metadata

def test_get_splits_and_rows():
    max_split_count = 5
    next_token = None
    all_splits = []
    schema = 'sales'
    table = 'orders'

    while True:
        # Get a batch of splits
        response = requests.post(f'{BASE_URL}/schemas/{schema}/tables/{table}/splits',
                                 json={'maxSplitCount': max_split_count, 'nextToken': next_token})
        assert response.status_code == 200
        split_batch = response.json()
        all_splits.extend(split_batch['splits'])
        next_token = split_batch['nextToken']

        # Test each split in the batch
        for split in split_batch['splits']:
            split_id = split['splitId']
            response = requests.post(f"{BASE_URL}/splits/{split_id}/rows", json={})
            assert response.status_code == 200
            row_data = response.json()
            assert row_data['rowCount'] == 1

        if next_token is None:
            break

    # Check if all splits cover the entire dataset
    assert len(all_splits) == 30
    assert all_splits[0]['hosts'][0]['host'] == 'localhost'
    assert all_splits[0]['hosts'][0]['port'] == 8080
    for split in all_splits:
        split_id = split['splitId']
        assert split_id.startswith(f"{schema}_{table}_")

if __name__ == '__main__':
    test_list_schemas()
    test_list_tables()
    test_get_table_metadata()
    test_get_splits_and_rows()
    print('All tests passed!')
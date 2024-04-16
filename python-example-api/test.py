import requests

BASE_URL = 'http://localhost:8080'

def test_list_schemas():
    response = requests.get(f'{BASE_URL}/schemas')
    assert response.status_code == 200
    assert response.json() == ['sales', 'inventory']

def test_list_tables():
    response = requests.get(f'{BASE_URL}/schemas/sales/tables')
    assert response.status_code == 200
    expected_tables = [
        {'schema': 'sales', 'table': 'orders'},
        {'schema': 'sales', 'table': 'customers'}
    ]
    assert sorted(response.json(), key=lambda x: x['table']) \
           == sorted(expected_tables, key=lambda x: x['table'])

def test_get_table_metadata():
    response = requests.get(f'{BASE_URL}/schemas/sales/tables/orders')
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
                             json={'maxSplitCount': max_split_count})
    assert response.status_code == 200
    splits = response.json()['splits']

    # Test each split in the batch
    for split in splits:
        response = requests.post(f"{BASE_URL}/schemas/{schema}/tables/{table}/splits/{split}/rows", json={})
        assert response.status_code == 200
        row_data = response.json()
        assert row_data['rowCount'] == 1

    # Check if all splits cover the entire dataset
    assert len(splits) == 30
    for split in splits:
        assert int(split) >= 0

if __name__ == '__main__':
    test_list_schemas()
    test_list_tables()
    test_get_table_metadata()
    test_get_splits_and_rows()
    print('All tests passed!')
import requests

BASE_URL = 'http://localhost:8080'

def test_list_schemas():
    response = requests.get(f'{BASE_URL}/schemas')
    assert response.status_code == 200
    assert response.json() == {'schemas': ['sales', 'inventory']}

def test_list_tables():
    response = requests.get(f'{BASE_URL}/schemas/sales/tables')
    assert response.status_code == 200
    assert response.json() == {'tables': ['orders', 'customers']}

def test_get_table_metadata():
    response = requests.get(f'{BASE_URL}/schemas/sales/tables/orders')
    assert response.status_code == 200
    expected_metadata = {
        'schema': 'sales',
        'table': 'orders',
        'columns': [
            {'name': 'order_id', 'type': 'string'},
            {'name': 'customer_id', 'type': 'string'},
            {'name': 'order_date', 'type': 'string'},
            {'name': 'total_amount', 'type': 'string'}
        ]
    }
    assert response.json() == expected_metadata

def test_get_splits_and_rows():
    max_split_count = 5
    next_token = None
    all_splits = []

    while True:
        # Get a batch of splits
        response = requests.post(f'{BASE_URL}/schemas/sales/tables/orders/splits',
                                 json={'maxSplitCount': max_split_count, 'nextToken': next_token})
        assert response.status_code == 200
        split_batch = response.json()
        all_splits.extend(split_batch['splits'])
        next_token = split_batch['nextToken']

        # Test each split in the batch
        for split in split_batch['splits']:
            response = requests.post(f"{BASE_URL}/splits/{split['splitId']}/rows",
                                     params={'schema': 'sales', 'table': 'orders'},
                                     json={'start': split['start'], 'end': split['end']})
            assert response.status_code == 200
            row_data = response.json()
            assert row_data['rowCount'] == 1

        if next_token is None:
            break

    # Check if all splits cover the entire dataset
    assert len(all_splits) == 30
    assert all_splits[0]['start'] == 0
    assert all_splits[-1]['end'] == 30

if __name__ == '__main__':
    test_list_schemas()
    test_list_tables()
    test_get_table_metadata()
    test_get_splits_and_rows()
    print('All tests passed!')
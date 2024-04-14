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

def test_get_splits():
    response = requests.post(f'{BASE_URL}/schemas/sales/tables/orders/splits', json={'maxSplitCount': 2})
    assert response.status_code == 200
    splits = response.json()['splits']
    assert len(splits) == 2
    assert splits[0]['start'] == 0
    assert splits[0]['end'] == 2
    assert splits[1]['start'] == 2
    assert splits[1]['end'] == 3

def test_get_rows():
    # Get splits
    response = requests.post(f'{BASE_URL}/schemas/sales/tables/orders/splits', json={'maxSplitCount': 2})
    splits = response.json()['splits']

    # Test split 0
    response = requests.post(f'{BASE_URL}/splits/0/rows', params={'schema': 'sales', 'table': 'orders'}, json=splits[0])
    assert response.status_code == 200
    expected_rows_split_0 = {
        'columnBlocks': [
            ['1', '2'],
            ['1', '2'],
            ['2023-01-01', '2023-01-02'],
            ['100.50', '75.80']
        ],
        'rowCount': 2,
        'nextToken': None
    }
    assert response.json() == expected_rows_split_0

    # Test split 1
    response = requests.post(f'{BASE_URL}/splits/1/rows', params={'schema': 'sales', 'table': 'orders'}, json=splits[1])
    assert response.status_code == 200
    expected_rows_split_1 = {
        'columnBlocks': [
            ['3'],
            ['1'],
            ['2023-01-03'],
            ['120.00']
        ],
        'rowCount': 1,
        'nextToken': None
    }
    assert response.json() == expected_rows_split_1

if __name__ == '__main__':
    test_list_schemas()
    test_list_tables()
    test_get_table_metadata()
    test_get_splits()
    test_get_rows()
    print('All tests passed!')
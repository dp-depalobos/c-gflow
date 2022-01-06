import pytest

import tests.test_helper as th
import main

@pytest.fixture(scope='module')
def test_client():
    flask_app = main.app
    testing_client = flask_app.test_client()
    ctx = flask_app.app_context()
    ctx.push()
    yield testing_client
    ctx.pop()

def test_handler_upload_original_csv_file(test_client):
    file = th.ORIGINAL_DATA_TEST
    data = {'file': (open(file, 'rb'), file)}
    response = test_client.post('/', data=data)
    assert th.ORIGINAL_TXT in response.get_data(as_text=True)

def test_handler_upload_one_row(test_client):
    file = th.ONE_ROW_TEST
    data = {'file': (open(file, 'rb'), file)}
    response = test_client.post('/', data=data)
    assert th.ONE_ROW_TXT in response.get_data(as_text=True)

def test_handler_upload_one_level_duplicate(test_client):
    file = th.ORIGINAL_ONE_LEVEL_TEST
    data = {'file': (open(file, 'rb'), file)}
    response = test_client.post('/', data=data)
    assert th.ONE_ROW_DUPLICATE_TXT in response.get_data(as_text=True)

def test_handler_upload_original_duplicate(test_client):
    file = th.DUPLICATE_RECORD_TEST
    data = {'file': (open(file, 'rb'), file)}
    response = test_client.post('/', data=data)
    assert th.ORIGINAL_DUPLICATE_TXT in response.get_data(as_text=True)

def test_handler_upload_upto_level_6(test_client):
    file = th.UPTO_LEVEL_6_TEST
    data = {'file': (open(file, 'rb'), file)}
    response = test_client.post('/', data=data)
    assert th.UPTO_LEVEL_6_TXT in response.get_data(as_text=True)

def test_handler_upload_upto_level_10(test_client):
    file = th.UPTO_LEVEL_10_TEST
    data = {'file': (open(file, 'rb'), file)}
    response = test_client.post('/', data=data)
    assert th.UPTO_LEVEL_10_TXT in response.get_data(as_text=True)
import pytest
import test_helper as th

import main

@pytest.fixture(scope='module')
def test_client():
    flask_app = main.app
    testing_client = flask_app.test_client()
    ctx = flask_app.app_context()
    ctx.push()
    yield testing_client
    ctx.pop()

def test_upload_textfile(test_client):
    file = th.TXT_FILE_TEST
    data = {'file': (open(file, 'rb'), file)}
    response = test_client.post('/', data=data)
    assert response.status_code == 302
    
def test_upload_valid_csv_file(test_client):
    file = th.ORIGINAL_DATA_TEST
    data = {'file': (open(file, 'rb'), file)}
    response = test_client.post('/', data=data)
    assert response.status_code == 200

def test_upload_empty_rows(test_client):
    file = th.EMPTY_ROWS_TEST
    data = {'file': (open(file, 'rb'), file)}
    response = test_client.post('/', data=data)
    assert response.status_code == 302

def test_upload_empty_file(test_client):
    file = th.EMPTY_FILE_TEST
    data = {'file': (open(file, 'rb'), file)}
    response = test_client.post('/', data=data)
    assert response.status_code == 302

def test_upload_base_data(test_client):
    file = th.BASE_DATA_TEST
    data = {'file': (open(file, 'rb'), file)}
    response = test_client.post('/', data=data)
    assert response.status_code == 302
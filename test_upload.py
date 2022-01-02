import os

import pytest
from werkzeug.wrappers.response import ResponseStream

import main

TXT_FILE = os.path.join("files", "samp.txt")
EMPTY_ROWS = os.path.join("files", "columns_only.csv")
EMPTY_FILE = os.path.join("files", "empty_file.csv")
ORIGINAL_DATA = os.path.join("files", "original_data.csv")
CURRENT = os.getcwd()

@pytest.fixture(scope='module')
def test_client():
    flask_app = main.app
    testing_client = flask_app.test_client()
    ctx = flask_app.app_context()
    ctx.push()
    yield testing_client
    ctx.pop()

def test_upload_textfile(test_client):
    file = os.path.join(CURRENT, TXT_FILE)
    data = {'file': (open(file, 'rb'), file)}
    response = test_client.post('/', data=data)
    assert response.status_code == 302
    
def test_upload_valid_csv_file(test_client):
    file = os.path.join(CURRENT, ORIGINAL_DATA)
    data = {'file': (open(file, 'rb'), file)}
    response = test_client.post('/', data=data)
    assert response.status_code == 200

def test_upload_empty_rows(test_client):
    file = os.path.join(CURRENT, EMPTY_ROWS)
    data = {'file': (open(file, 'rb'), file)}
    response = test_client.post('/', data=data)
    assert response.status_code == 500

def test_upload_empty_file(test_client):
    file = os.path.join(CURRENT, EMPTY_FILE)
    data = {'file': (open(file, 'rb'), file)}
    response = test_client.post('/', data=data)
    assert response.status_code == 500
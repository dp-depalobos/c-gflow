python3 -m virtualenv venv
CALL .venv\scripts\activate
pip install -r requirment.txt
py.test
PAUSE
#!/bin/sh

sudo apt install python3.8-venv
python3 -m venv venv
activate(){
    . venv/bin/activate
}
activate

pip install -r requirements.txt && py.test
#!/bin/sh

echo $"Installing pyenv version"
pyenv install 3.8.12

echo $"Creating venv"
pyenv virtualenv 3.8.12 py_3_8_12
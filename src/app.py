from flask import Flask
import os

current = os.getcwd()
upload_folder = os.path.join(current, "temp")

app = Flask(__name__)
app.secret_key = "secret key"
app.config['UPLOAD_FOLDER'] = upload_folder
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024
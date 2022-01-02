import os

from src.app import app
import csv
from flask import flash, request, redirect, render_template
from werkzeug.utils import secure_filename

from src.handler import trigger
ALLOWED_EXTENSION = 'csv'
def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() == ALLOWED_EXTENSION

def validate(path):
    row_count = 0
    with open(path) as csv_file:
        csv_reader = csv.DictReader(csv_file)
        rows = list(csv_reader)
        row_count = len(rows)
    return row_count

@app.route('/', methods=['POST'])
def upload_file():
    result = ''
    if request.method == 'POST':
        if "file" not in request.files:
            flash('No file part')
            return redirect(request.url)
        file = request.files['file']
        if file.filename == '':
            flash('No file selected for uploading')
            return redirect(request.url)
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            if validate(filepath) == 0:
                flash('Empty file/rows')
                return redirect(request.url)            
            else:
                file.save(filepath)
                result = trigger(filepath)
                flash('File successfully uploaded')
                return render_template('upload.html', filename=result)
        else:
            flash('Allowed file type: csv')
            return redirect(request.url)

@app.route('/')
def upload_form():
    return render_template("upload.html")

if __name__ == "__main__":
    app.run(debug=True)
    
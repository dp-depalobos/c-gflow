import csv
import os
import io
import copy

from src.app import app
from src.csv_operator import CsvOperator as csv_opr
from flask import flash, request, redirect, render_template
from werkzeug.utils import secure_filename

from src.handler import handle
ALLOWED_EXTENSION = "csv"

def allowed_file(filename):
    return "." in filename and \
        filename.rsplit(".", 1)[1].lower() == ALLOWED_EXTENSION

@app.route("/", methods=["POST"])
def upload_file():
    result = ""
    if request.method == "POST":
        if "file" not in request.files:
            flash("No file part")
            return redirect(request.url)
        file = request.files["file"]
        if file.filename == "":
            flash("No file selected for uploading")
            return redirect(request.url)
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            filepath = os.path.join(app.config["UPLOAD_FOLDER"], filename)
            file.save(filepath)
            c_opr = csv_opr(filepath)
            if not c_opr.validate_len():
                flash("Empty file/rows")
                os.remove(filepath)
                return redirect(request.url)
            if not c_opr.validate_header():
                flash("Missing mandatory columns")
                os.remove(filepath)
                return redirect(request.url)
            else:
                result = handle(filepath)
                os.remove(filepath)
                flash("File successfully uploaded")
                return render_template("upload.html", filename=result)
        else:
            flash("Allowed file type: CSV")
            return redirect(request.url)

@app.route("/")
def upload_form():
    return render_template("upload.html")

if __name__ == "__main__":
    app.run(debug=True)
from src.app import app
from src.csv_operator import CsvOperator as csv_opr
from flask import flash, request, redirect, render_template

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

        if not allowed_file(file.filename):
            flash("Allowed file type: CSV")
            return redirect(request.url)

        c_opr = csv_opr(file.read())
        if not c_opr.validate_len():
            flash("Empty file/rows")
            return redirect(request.url)

        if not (c_opr.valid_header and c_opr.depth > 0):
            flash("Missing mandatory columns")
            return redirect(request.url)

        result = handle(c_opr.columns, c_opr.rows, c_opr.depth)
        flash("File successfully uploaded")
        return render_template("upload.html", filename=result)

@app.route("/")
def upload_form():
    return render_template("upload.html")

if __name__ == "__main__":
    app.run(debug=True)
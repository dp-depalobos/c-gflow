import csv

MANDATORY_HEADERS = ["Level 1 - Name",
                     "Level 1 - ID",
                     "Level 1 - URL"]

class CsvOperator:
    def __init__(self, path):
        csv_file = open(path)
        reader = csv.DictReader(csv_file)
        self.columns = reader.fieldnames
        self.rows = self.copy_reader(reader)
    
    def copy_reader(self, reader):
        """ Stores contents of csv to list. Since after a DictReader 
            row is read, it is no longer accessible """
        rows = []
        for row in reader:
            rows.append(row)
        return rows

    def validate_len(self):
        return len(self.rows) > 0
    
    def validate_header(self):
        """ Check if the csv file contains mandatory columns """
        if all(hdr in self.columns for hdr in MANDATORY_HEADERS):
            return True
        return False
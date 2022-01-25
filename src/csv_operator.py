import csv
import io

MANDATORY_HEADERS = ["Name", "ID", "URL"]

class CsvOperator:
    def __init__(self, file):
        self.header = self.get_header(file)
        self.rows = self.get_rows(file)
        self.depth = self.calc_depth()
        self.valid_header = self.validate_headers()

    def get_header(self, raw_data):
        """Gets the header row from the CSV file"""
        raw_data_lst = raw_data.splitlines()
        return io.StringIO(raw_data_lst[0].decode()).getvalue().split(",")

    def get_rows(self, raw_data):
        """Gets the data rows from the CSV file"""
        raw_data_lst = raw_data.splitlines()
        data_lst = []
        for data in raw_data_lst[1:]:
            row = io.StringIO(data.decode())
            reader = csv.reader(row, delimiter=",")
            for line in reader:
                data_lst.append(dict(zip(self.header, line)))
        return data_lst

    def calc_depth(self):
        """Computes for the maximum level for the given CSV"""
        depth = 0
        for header in self.header:
            if "id" in header.lower():
                depth += 1
        return depth

    def validate_headers(self):
        """Checks if all mandatory header exists for all levels"""
        for depth in range(1, self.depth+1):
            pattern = str(depth) + " "
            columns = [column for column in self.header if pattern in column]
            col_str = "".join(columns)
            for header in MANDATORY_HEADERS:
                if header in col_str:
                    continue
                else:
                    return False
        return True

    def validate_len(self):
        """Checks if there are data rows"""
        return len(self.rows) > 0
import csv
import io

MANDATORY_HEADERS = ["Name", "ID", "URL"]

class CsvOperator:
    def __init__(self, file):
        self.columns = self.get_header(file)
        self.rows = self.get_rows(file)
        self.depth = self.calc_depth(self.columns)
        self.valid_header = self.validate_headers(self.columns)

    def get_header(self, raw_data):
        raw_data_lst = raw_data.splitlines()
        return io.StringIO(raw_data_lst[0].decode()).getvalue().split(",")
    
    def get_rows(self, raw_data):
        raw_data_lst = raw_data.splitlines()
        lst = []
        for data in raw_data_lst[1:]:
            row = io.StringIO(data.decode())
            reader = csv.reader(row, delimiter=",")
            for i in reader:
                lst.append(dict(zip(self.columns, i)))
        return lst

    def calc_depth(self, headers):
        depth = 0
        for i in headers:
            if "id" in i.lower():
                depth += 1
        return depth

    def validate_headers(self, headers):
        for depth in range(1, self.depth + 1):
            pattern = str(depth) + str(" ")
            cols = [col for col in headers if pattern in col]
            col_str = "".join(cols)
            for i in MANDATORY_HEADERS:
                if i in col_str:
                    continue
                else:
                    return False            
        return True

    def validate_len(self):
        return len(self.rows) > 0
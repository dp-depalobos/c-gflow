import json

class JsonOperator:
    
    def create_json_string(self, result):
        """ Converts the list of dictionary into a single JSON String """    
        json_str = ""
        for i in range(len(result)):
            json_str += json.dumps(result[i])
        return json_str
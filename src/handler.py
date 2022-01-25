from flask import Flask

from src.node_operator import NodeOperator as node_opr
from src.json_operator import JsonOperator

app = Flask(__name__)
app.secret_key = "secret key"
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024

def handle(columns, rows, depth):
    """ An explanation of the logic is included in the readme file"""
    depth_node = node_opr().create_depth_node(depth,
                                         rows,
                                         columns)

    if depth == 1:
        end_node = list(depth_node[1].values())
    else:
        end_node = node_opr().combine_nodes(depth_node, depth)

    return JsonOperator().create_json_string(end_node)
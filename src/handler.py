from src.csv_operator import CsvOperator as csv_opr
from src.node_operator import NodeOperator as node_opr
from src.json_operator import JsonOperator as json_opr

def handle(path):
    """ An explanation of the logic is included in the readme file"""
    c_opr = csv_opr(path)
    rows = c_opr.rows
    columns = c_opr.columns

    n_opr = node_opr()
    max_depth = n_opr.calc_max_depth(columns)
    depth_node = n_opr.create_depth_node(max_depth,
                                         rows,
                                         columns)

    if max_depth == 1:
        end_node = list(depth_node[1].values())
    else:
        end_node = n_opr.combine_nodes(depth_node, max_depth)

    j_opr = json_opr()
    result = j_opr.create_json_string(end_node)
    return result
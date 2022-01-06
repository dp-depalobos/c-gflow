from src.node_operator import NodeOperator as node_opr
from src.json_operator import JsonOperator as json_opr

def handle(columns, rows, depth):
    """ An explanation of the logic is included in the readme file"""
    n_opr = node_opr()
    depth_node = n_opr.create_depth_node(depth,
                                         rows,
                                         columns)

    if depth == 1:
        end_node = list(depth_node[1].values())
    else:
        end_node = n_opr.combine_nodes(depth_node, depth)

    j_opr = json_opr()
    result = j_opr.create_json_string(end_node)
    return result
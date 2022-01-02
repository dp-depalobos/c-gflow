import os
from pyspark.sql.types import ArrayType, StringType, StructField, StructType
from pyspark.sql.functions import col
import json

from src.dfdecorator import Decorator
from src.initializer import Initializer
from src.getter import Getter

dec = Decorator()
init = Initializer()
getter = Getter()

def handle(path, spark):
    df = spark.read.csv(path, header="true")
    max_depth = dec.calc_max_depth(df)
    depth_node_dict = create_depth_node_dict(df, max_depth)
    if max_depth == 1:
        node = depth_node_dict[1]
        filled = dec.append_empty_children(node)
        result = dec.concat_columns(filled)
    else:
        result = combine_nodes(depth_node_dict, max_depth)
    return create_result(result, spark)

def create_depth_node_dict(df, max_depth):
    depth_node_dict = {}
    for i in range(max_depth, 0, -1):
        columns = getter.get_column_names(df, i)
        if i == 1:
            temp = getter.get_first_node( \
                                df, columns[0], \
                                columns[1], columns[2])
            depth_node_dict[i] = dec.remove_duplicates(temp)
        else:
            parent_id = 'Level ' + str(i-1) + str(' - ID')
            temp = getter.get_node(\
                                df, parent_id, columns[0], \
                                columns[1], columns[2])
            depth_node_dict[i] = dec.remove_duplicates(temp)
    return depth_node_dict

def join_parent_child(left, right):
    return left.join(\
        right, left.ID == right.tempID, 'left')

def combine_nodes(depth_node_dict, max_depth):
    agg = {}

    for i in range(max_depth, 0, -1):
        if max_depth == i:
            node = depth_node_dict[i]
            filled = dec.append_empty_children(node)
            formatted = dec.concat_columns(filled).orderBy(col("concat"))
            agg[i] = dec.aggregate_child_node(formatted)
        elif i == 1:
            joined = join_parent_child(depth_node_dict[i], agg[i+1])
            filled = dec.fill_null_children(joined)
            agg[i] = filled
        else:
            joined = join_parent_child(depth_node_dict[i], agg[i+1])
            filled = dec.fill_null_children(joined)
            formatted = dec.concat_columns(filled).orderBy(col("concat"))
            agg[i] = dec.aggregate_child_node(formatted)
    return agg[i]

def create_result(df, spark):
    if df:
        result = df.select('label', 'ID', 'link', 'children')
    else:
        schema = StructType([ \
            StructField('label', StringType()), \
            StructField('ID', StringType()), \
            StructField('link', StringType()), \
            StructField('children', ArrayType(StructType()))])
        result = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)
    return result

def write_output(result):
    current = os.getcwd()
    output_path = os.path.join(current, "webout")
    result.write.json(path=output_path, ignoreNullFields=False, mode='overwrite')

def trigger(path):
    spark = init.initialize()
    df = handle(path, spark)
    result = create_json_string(df)
    return result

import json
def create_json_string(result):
    temp = result.collect()
    cont = []
    for i in temp:
        cont.append(i.asDict(True))
    return json.dumps(cont)


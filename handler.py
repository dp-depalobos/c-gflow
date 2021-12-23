from pyspark.sql.types import ArrayType, StringType, StructField, StructType

from decorator import Decorator
from initializer import Initializer
from getter import Getter

getter = Getter()
dec = Decorator()

def create_depth_node_dict(df, max_depth):
    depth_node_dict = {}
    for i in range(max_depth, 0, -1):
        if i == 1:
            columns = getter.get_column_names(df, i)
            depth_node_dict[i] = getter.get_first_node(\
                df, columns[0], columns[1], columns[2], i)
        else:
            columns = getter.get_column_names(df, i)
            parent_id = 'Level ' + str(i-1) + str(' - ID')
            depth_node_dict[i] = getter.get_subsequent_node(\
                df, parent_id, columns[0], columns[1], columns[2], i)
    return depth_node_dict

def combine_nodes(depth_node_dict, max_depth):
    agg = {}
    if max_depth == 1:
        formatted = dec.concat_columns(depth_node_dict[1], True)
        agg[1] = formatted
        return agg

    for i in range(max_depth, 0, -1):
        if max_depth == i:
            formatted = dec.concat_columns(depth_node_dict[i], True)
            agg[i] = dec.aggregate_child_node(formatted)
        elif i == 1:
            agg[i] = depth_node_dict[i].join(\
                agg[i+1], depth_node_dict[i].ID == agg[i+1].tempID, 'left')
        else:
            merged = depth_node_dict[i].join(\
                agg[i+1], depth_node_dict[i].ID == agg[i+1].tempID, 'left')
            formatted = dec.concat_columns(merged)
            agg[i] = dec.aggregate_child_node(formatted)
    return agg

def create_result(agg, spark):
    if agg:
        result = agg[1].select('label', 'ID', 'link', 'children')
    else:
        schema = StructType([\
            StructField('label', StringType(), True),\
            StructField('ID', StringType(), True),\
            StructField('link', StringType(), True),\
            StructField('children', ArrayType(StructType()), True)])
        result = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)
    return result

def handle(path, spark):
    df = spark.read.csv(path, header="true")
    df = dec.add_depth_column(df)

    max_depth = dec.calc_max_depth(df)
    depth_node_dict = create_depth_node_dict(df, max_depth)
    agg = combine_nodes(depth_node_dict, max_depth)
    return create_result(agg, spark)

def write_output(result, path = "/home/dindo/client/data.json"):
    result.write.json(path=path, ignoreNullFields=False, mode='overwrite')

# path = "/home/dindo/t2/c-gflow/test_csvs/level_four.csv"
# init = Initializer()
# spark = init.initialize()
# res = handle(path, spark)
# write_output(res)
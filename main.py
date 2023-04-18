import os
import sys
import pyspark.sql.functions as f
from pyspark.sql import Window

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


def get_distinct_count(local_df):
    dist_count = local_df.agg(f.countDistinct('*')).collect()[0][0]
    return dist_count


def sum_of_column(local_df, col_name):
    sum_column = local_df.agg(f.sum(col_name)).collect()[0][0]
    return sum_column


def sum_of_column_on_partition(local_df, partition_by_cols_list, col_name):
    window = Window.partitionBy(partition_by_cols_list)
    local_df = local_df.select('*', f.sum(f.col(col_name)).over(window).alias('sum_column'))
    return local_df

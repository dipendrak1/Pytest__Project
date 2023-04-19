import os
import sys
import pyspark.sql.functions as f
from pyspark.sql import Window

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


def get_distinct_count(local_df):
    return local_df.distinct().count()


def sum_of_column(local_df, col_name):
    sum_column = local_df.agg(f.sum(col_name)).collect()[0][0]
    return sum_column


def sum_of_column_on_partition(local_df, partition_by_cols_list, col_name):
    window = Window.partitionBy(partition_by_cols_list)
    local_df = local_df.select('*', f.sum(f.col(col_name)).over(window).alias('sum_column'))
    return local_df


def add_new_row_to_df(local_spark, local_df, rows_to_append):
    """rows_to_append example format = [(1, 2, 3, 'a'), (4, 5, 6, 'b')]"""
    new_row = local_spark.createDataFrame(rows_to_append, local_df.columns)
    return local_df.union(new_row)

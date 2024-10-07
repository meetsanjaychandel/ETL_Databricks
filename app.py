import os

from databricks.connect import DatabricksSession
from pyspark import SparkConf
from pyspark.sql.functions import col, to_date

from util import connect_to_cluster
from read import from_files
from process import transform_df
from  write import write_df
def main():
    src_dir=os.environ.get("SRC_DIR")
    src_file_pattern= f'{os.environ.get("SRC_FILE_PATTERN")}'
    src_file_format= os.environ.get("SRC_FILE_FORMAT")

    tgt_dir = os.environ.get("TGT_DIR")
    tgt_file_format = os.environ.get("TGT_FILE_FORMAT")

    # spark=connect_to_cluster('DEV')
    spark = DatabricksSession.builder.getOrCreate()

    df = from_files(spark,src_dir,src_file_pattern,src_file_format)
    df_transformed = transform_df(df)
    write_df(df_transformed,tgt_dir,tgt_file_format)

    outputdf = spark.read.format('parquet').option('path',
                                                'dbfs:/mnt/raw/ghactivity/').load()
    outputdf.filter(col("created_at") >= "2021-01-14").groupBy(col("created_at").cast("date").alias("created_at")).count().show()

if __name__=="__main__":
    main()
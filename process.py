from pyspark.sql.functions import year,month,dayofmonth

def transform_df(df):
    transformedDf = df.withColumn("year",year("created_at")) \
                    .withColumn("month",month("created_at")) \
                    .withColumn("day",dayofmonth("created_at")) \
                    # .select('created_at','year','month','day')

    return transformedDf
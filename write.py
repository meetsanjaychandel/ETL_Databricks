def write_df(df,tgt_dir,file_format):

    df.write \
        .format(file_format) \
        .partitionBy("year","month","day") \
        .mode(saveMode="update") \
        .option("path",tgt_dir) \
        .save()

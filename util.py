import os
from databricks.connect import DatabricksSession

def set_cluster_config(cluster_env):
    if cluster_env == 'DEV':
        os.environ['DATABRICKS_HOST'] = 'https://<databricks-instance-1>.cloud.databricks.com'
        os.environ['DATABRICKS_TOKEN'] = '<your-token-1>'
        os.environ['DATABRICKS_CLUSTER_ID'] = '<cluster-id-1>'
    elif cluster_env == 'PROD':
        os.environ['DATABRICKS_HOST'] = 'https://<databricks-instance-2>.cloud.databricks.com'
        os.environ['DATABRICKS_TOKEN'] = '<your-token-2>'
        os.environ['DATABRICKS_CLUSTER_ID'] = '<cluster-id-2>'

def connect_to_cluster(env):
    set_cluster_config(env)
    # Create Spark session after setting environment variables
    spark = DatabricksSession.builder.getOrCreate()
    return spark


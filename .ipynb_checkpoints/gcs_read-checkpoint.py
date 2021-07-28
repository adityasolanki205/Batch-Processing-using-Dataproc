
from pyspark.sql import SparkSession
from google.cloud import storage


appName = "DataProc testing"
master = "local"
spark = SparkSession.Builder.\
        appName(appName).\
        master(master).\
        config('spark.jars.packages', 
                   'com.google.cloud.bigdataoss:gcs-connector:hadoop2-1.9.17').\
        config('spark.jars.excludes',
                   'javax.jms:jms,com.sun.jdmk:jmxtools,com.sun.jmx:jmxri').\
        config('spark.driver.userClassPathFirst','true').\
        config('spark.executor.userClassPathFirst','true').\
        config('spark.hadoop.fs.gs.impl',
                   'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem').\
        config('spark.hadoop.fs.gs.auth.service.account.enable', 'false').\
        getOrCreate()     


df = spark.read.csv("gs://dataProc-testing/titanic.csv",header=True, inferSchema=True)
print(df.schema)


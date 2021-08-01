import pyspark
from pyspark.sql import SparkSession

appName = "DataProc testing"
master = "local"
spark = SparkSession.builder.\
        appName(appName).\
        master(master).\
        getOrCreate()     

bucket = "dataproc-testing-pyspark"
spark.conf.set('temporaryGcsBucket', bucket)
df = spark.read.option( "inferSchema" , "true" ).option("header","true").csv("gs://dataproc-testing-pyspark/titanic.csv")
df.createOrReplaceTempView('Titanic')

complete_data = spark.sql('Select * from Titanic')
complete_data.show()
complete_data.write.format('com.google.cloud.spark.bigquery').option('table', 'titanic.titanic_data').mode('append').save()


import pyspark
from pyspark.sql import SparkSession

appName = "DataProc testing"
master = "local"
spark = SparkSession.builder.\
        appName(appName).\
        master(master).\
        getOrCreate()     

bucket = "gs://dataproc-testing"
spark.conf.set('temporaryGcsBucket', bucket)
df = spark.read.option( "inferSchema" , "true" ).option("header","true").csv("gs://dataproc-testing/titanic.csv")
df.createOrReplaceTempView('Titanic')

complete_data = sparl.sql('Select * from Titanic')
complete_data.show()
complete_data.printschema()
complete_data.write.format('bigquery').option('table', 'titanic.titanic_data').save()


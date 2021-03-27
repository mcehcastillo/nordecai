#pyspark code for avdata exploration
# append: 'export PYSPARK_PYTHON=python3' to .bashrc
#console nano ~/.bashrc

from pyspark.sql import SparkSession
import pyspark.sql.functions as fun


# Set up SparkSession
spark = SparkSession.builder \
  .master("local[8]") \
  .appName("avdata") \
  .getOrCreate()
  
df = spark.read.format("csv").options(header='True',inferSchema='True').load("avdata/*")
df.show()

df.count()

# get sourcefile name from input_file_name()
df = df.withColumn("path", fun.input_file_name())
regex_str = "[\/]([^\/]+[^\/]+)$" #regex to extract text after the last / or \
df = df.withColumn("sourcefile", fun.regexp_extract("path",regex_str,1))
df.show()

#######################################################################
# handle dates and times
df=df.withColumn('timestamp', fun.to_date("timestamp"))
df.show(2)

# now we should be able to convert or extract date features from timestamp
df.withColumn('dayofmonth', fun.dayofmonth("timestamp")).show(2)
df.withColumn('month', fun.month("timestamp")).show(2)
df.withColumn('year', fun.year("timestamp")).show(2)
df.withColumn('dayofyear', fun.dayofyear("timestamp")).show(2) 

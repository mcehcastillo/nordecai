#pyspark code for avdata exploration
# append: 'export PYSPARK_PYTHON=python3' to .bashrc

from pyspark.sql import SparkSession
import pyspark.sql.functions as fun

spark = SparkSession.builder \
    .master("local[8]") \
    .appName("avdata") \
    .getOrCreate()
    
# warning: It is a little slower to read the data with inferSchema='True'. 
#Consider defining schema.

df = spark.read.format("csv").options(header='True',inferSchema='True').load("avdata/*")
df.printSchema()

#get a count of rows
df.count()

# get sourcefile name from input_file_name()
df = df.withColumn("path", fun.input_file_name())
regex_str = "[\/]([^\/]+[^\/]+)$" #regex to extract text after the last / or \ 
#any workarounds to using regex?
df = df.withColumn("sourcefile", fun.regexp_extract("path",regex_str,1))
#why do you need anothr extract after the last one? unsure what this is doing
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

# calculate the difference from the current date ('days_ago')
df.withColumn('days_ago', fun.datediff(fun.current_date(), "timestamp")).show()

#getting different parts of day, always showing 2 output, can this be done in one function?
#is this extracting every date from every row and then comparing it to the current date, or is it selecting a 
random date?

########################################################################
#group_by
# summarize within group data
df.groupBy("sourcefile").count().show(99)
df.groupBy("sourcefile").min('open').show(99)
df.groupBy("sourcefile").mean('open').show(99)
df.groupBy("sourcefile").max('open','close').show(99)



########################################################################
#window functions
from pyspark.sql.window import Window
df=df.withColumn('days_ago', fun.datediff(fun.current_date(), "timestamp"))

windowSpec  = Window.partitionBy("sourcefile").orderBy("days_ago")

#see also lead
dflag=df.withColumn("lag",fun.lag("open",14).over(windowSpec))
dflag.select('sourcefile', 'lag', 'open').show(99)

dflag.withColumn('twoweekdiff', fun.col('lag') - fun.col('open')).show() 
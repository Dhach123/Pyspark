# Last amended date: 11/07/2019
#		1. Analyse data streaming into spark from files
#		    Use Structured spark streaming

# Usage Steps:
#		     Step 1.     Start hadoop
#		      Step 2.   On one terminal, first run the filegenerator program as:
#
#			                 $ cd /home/ashok/Documents/spark/streaming/streamfromfiles
#			                 $  ./file_gen.sh
#
#                  Step3.  Next, open another terminal and  type:
#                    $ cd ~
#                    $ spark-submit /home/ashok/Documents/spark/streaming/streamfromfiles/stream_data_fromfiles2.py
#                   OR
#                    $ spark-submit /home/ashok/Documents/spark/streaming/streamfromfiles/stream_data_fromfiles2.py  > /home/ashok/Documents/streamresults.txt

#
#	Then examine results.txt and you will find counts
#       cat   /home/ashok/Documents/streamresults.txt


# Call libraries
#  1.0 Library to generate Spark context
#          whether local  or on hadoop
from pyspark.context import SparkContext

# 1.1  Library to generate SparkSession
from pyspark.sql.session import SparkSession

# 1.2 Some type to define data schema
from pyspark.sql.types import StructType

# 2.0 Create spark context and session
#sc = SparkContext('local')
spark = SparkSession(sc)

# 2.1 Where will be my csv files which spark will analyse
#datafiles = "/home/ashok/Documents/spark/data"
datafiles = "hdfs://localhost:9000/user/ashok/data_files/fake"

# 3.0 CSV file structure
userSchema = StructType().add("name", "string").add("age", "integer")

# 4.0
# Stream files from folder /home/ashok/Documents/spark/data. 
#    New content must be added to new files.
# Ref:  https://stackoverflow.com/questions/45086501/how-to-process-new-records-only-from-file

csvDF = spark.readStream.option("sep", ";").schema(userSchema).csv(datafiles)

# 4.1 
abc = csvDF.select("name", "age").where("age > 40")

query = abc.writeStream.format("memory").outputMode("append").format("console").start()

query.awaitTermination()


# Last amended:   15th Dec, 2021
# Ref: https://spark.apache.org/docs/latest/streaming-programming-guide.html#a-quick-example
# Ref: https://github.com/apache/spark/blob/master/docs/structured-streaming-programming-guide.md
# API ref: http://spark.apache.org/docs/2.1.0/api/python/pyspark.sql.html

# Simple Spark streaming and aggregation
#
# Objective:
#		1. Analyse data streaming into spark from files
#		   Use Structured spark streaming
#                  We use SQL language to aggregate streams

# Usage Steps:
#		     Step 1.   Start hadoop
#		     Step 2.   On one terminal, first run the filegenerator program as:
#                              (copy and paste). Leave this terminal open.
"""

rm -f  $HOME/Documents/streamresults.txt
cd     $HOME/Documents/spark/3.streaming/2.streamfromfiles
chmod +x file_gen.sh
./file_gen.sh


"""
#       Step3.  Next, open another terminal and  type:
#            $ cd ~
#            $ spark-submit /home/ashok/Documents/spark/streaming/2.streamfromfiles/2.stream_data_fromfiles_sql.py
#       OR

"""

cd ~
export resultfile=$HOME/Documents/streamresults.txt
rm -f $resultfile
spark-submit  $HOME/Documents/spark/3.streaming/2.streamfromfiles/2.stream_data_fromfiles_sql.py  > $resultfile


"""
#
#                   ELSE,
#                   run on pyspark (py_spark:  see .bashrc)
#                   from para #2.1 onwards

#
#	Then examine on   terminal,
#       file 'streamresults.txt'  and you will find counts
#        The following bash-script on its own  periodically
#           reads the file

"""

while true; do 
         echo "Next reading of file..........."
         echo "==================== "
	 echo " "
         sleep 2
         cat  $HOME/Documents/streamresults.txt
         sleep 3
done

hdfs dfs -ls  /user/ashok/data_files/fake

"""

# Call libraries
#  1.0 Library to generate Spark context
#      whether local  or on hadoop

from pyspark.context import SparkContext

# 1.1  Library to generate SparkSession

from pyspark.sql.session import SparkSession

# 1.2 Some type to define data schema

from pyspark.sql.types import StructType

# 2.0 Create spark context and session

sc = SparkContext('local')
spark = SparkSession(sc)

# 2.1 Where will be my csv files which spark will analyse

from pyspark.sql.types import StructType
datafilesPath = "hdfs://localhost:9000/user/ashok/data_files/fake"

# 3.0 CSV file structure

userSchema = StructType()     \
                      .add("name", "string")      \
                      .add("balance", "integer")       

# 4.0
# Stream files from folder /home/ashok/Documents/spark/data. 
#    New content must be added to new files.
# Ref:  https://stackoverflow.com/questions/45086501/how-to-process-new-records-only-from-file

## Micro-batch trigger time?
# Trigger time for micro-batches can be specified. There are a number of options
#  See here: https://stackoverflow.com/questions/57760563/how-to-specify-batch-interval-in-spark-structured-streaming

csvDF = spark      \
            .readStream \
            .option("sep", ";")  \
            .schema(userSchema)  \
            .csv(datafilesPath)  \

# 4.1 Print data schema

csvDF.printSchema();

# 4.2
#  Register a streaming DataFrame/Dataset as a temporary view
#  and then apply SQL commands on it

csvDF.createOrReplaceTempView("updates")

# 4.3 Perform some selection and aggregation using SQL
#         Multiple streaming aggregations are not supported
# 	     abc is returns another streaming DF

abc = spark.sql("select name, avg(balance) as BALANCE from updates where balance >40  group by name")  


# 4.4 Print result to console.  'append' mode also exists but is not supported when
#     there are aggregations. 
#     format("memory")  =>The output is stored in memory as an in-memory table
#     format("console") => Prints the output to the console every time there is a trigger.

result = abc.writeStream \
               .format("memory")         \
               .outputMode("complete")   \
               .format("console")        \
               .start()

# 4.5
result.awaitTermination()

##################################################################

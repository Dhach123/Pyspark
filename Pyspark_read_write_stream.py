# Last amended: 15th Dec, 2021
# Ref: https://spark.apache.org/docs/latest/streaming-programming-guide.html#a-quick-example
# Ref: https://github.com/apache/spark/blob/master/docs/structured-streaming-programming-guide.md
# API ref: http://spark.apache.org/docs/2.1.0/api/python/pyspark.sql.html

#  Structured Spark streaming and aggregation
#
# Objective:
#		1. Analyse data streaming into spark from files
#		    Use Structured spark streaming

# Usage Steps:
#		     Step 1.   Start hadoop
#		     Step 2.   On one terminal, first run the filegenerator program as:
#                              (copy and paste). Leave this terminal open.
#                              Sample of data generated is:
# 				Kor;38
# 				Eth;65
# 				Bur;39
"""
# First, install 'Faker', if not installed: pip install Faker

rm -f  $HOME/Documents/streamresults.txt
cd     $HOME/Documents/spark/3.streaming/2.streamfromfiles
chmod +x file_gen.sh
./file_gen.sh


"""
#
#                  Step3.  Next, open another terminal and  type:
# 		  	   This step also stores file in hadoop. But not sure where.  
   
"""

cd ~
spark-submit $HOME/Documents/spark/3.streaming/2.streamfromfiles/1.stream_data_fromfiles1.py

"""
#                  OR, Better still
#                  Step3. Direct output to a file

"""

cd ~
export resultfile=$HOME/Documents/streamresults.txt
rm -f $resultfile
spark-submit  $HOME/Documents/spark/3.streaming/2.streamfromfiles/1.stream_data_fromfiles1.py  > $resultfile


"""
#                   ELSE,
#                   run on pyspark (py_spark:  see .bashrc)
#                   from para #2.1 onwards

#
#	Then examine on   terminal,
#       file 'streamresults.txt'  and you will find counts
#       The following bash-script outputs contents of file
#         periodically on console


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

### Call libraries
#  1.0 Library to generate Spark context
#          whether local  or on hadoop

from pyspark.context import SparkContext

# 1.1  Library to generate SparkSession

from pyspark.sql.session import SparkSession

# 1.2 Some type to define data schema

from pyspark.sql.types import StructType

# 2.0 Create spark context and session:

sc = SparkContext('local')
spark = SparkSession(sc)

# 2.1 Where will be my csv files which spark will analyse
#        Path should be on hadoop

datafilesPath = 'hdfs://localhost:9000/user/ashok/data_files/fake'

# 3.0 Define CSV file structure:

"""

There are many ways to define a StructType()
One way is to use  add() method. There are 
many add methods(). See the reference, below:

    https://spark.apache.org/docs/1.5.0/api/java/org/apache/spark/sql/types/StructType.html#add(java.lang.String,%20org.apache.spark.sql.types.DataType)

One add() method is:

  StructType()           \
   .add("a", "int")     \
   .add("b", "long")  \
   .add("c", "string")

Another add() as:

 StructType()           \
   .add("a", "int", true)  \
   .add("b", "long", false )  \
   .add("c", "string", true)


Another  add() as:

StructType() .add("a", IntegerType, true) .add("b", LongType, false) .add("c", StringType, true)

And there are others also.

"""

# 3.0 Define data schema in either of the following ways.
#     Schema definition is MUST. inferSchema will not work.

## 3.1 
from pyspark.sql.types import StructType

# 3.1.1
userSchema = StructType()                                                            \
                         .add("name", "string")             \
                         .add("balance", "integer")      


# # 3.2.1 OR, better, as:

# Note just using StringType instead of StringType()
#  will give error. Last 'boolean' refers to nullable
#   or not.

from pyspark.sql.types import StructType, IntegerType, StringType

# 3.2.2
userSchema = StructType()  \
                         .add("name", StringType(), True)  \
                         .add("balance",IntegerType(),True)


 
# 4.0
# Stream files from folder /home/ashok/Documents/spark/data. 
#    New content must be added to new files.
# Ref:  https://stackoverflow.com/questions/45086501/how-to-process-new-records-only-from-file

## Micro-batch trigger time?
# Trigger time for micro-batches can be specified. There are a number of options
#  See here: https://stackoverflow.com/questions/57760563/how-to-specify-batch-interval-in-spark-structured-streaming
#  if you do not set a batch interval, Spark will look for data as soon as it has written last batch.
#  Trigger option is available with writeStream() method.

csvDF = spark   \
              .readStream \
              .option("sep", ";")   \
              .schema(userSchema)   \
              .csv(datafilesPath)                  


"""
Another way to read stream:

Ref: https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.streaming.DataStreamReader

csvDF = spark.readStream.csv(
                              datafilesPath,
                              schema = userSchema,
                              sep = ",",
                              inferSchema=False
                             )



"""


# 4.1 Print data schema
csvDF.printSchema();


# 5.0 Perform some selection and aggregation using dataframe csvDF
#     Multiple streaming aggregations are not supported

abc = csvDF \
           .select("name", "balance") \
           .where("balance > 40")                   

abc = abc   \
        .groupby("name") \
        .agg({ 'balance' :   'avg' ,  '*' :  'count'  })

# 5.1 Print result to console.  'append' mode also exists but is not supported when
#     there are aggregations.  Output is sent to console but
#     directed to a file.


result = abc.writeStream  \
            .format("memory")   \
            .outputMode("complete") \
            .format("console")  \
            .start()


# 5.2
result.awaitTermination()

##################################################################

# Last amended: 3rd Dec, 2022
# Myfolder: $HOME/Documents/spark/3.streaming/1.readfromnetwork
# Ref: https://spark.apache.org/docs/latest/streaming-programming-guide.html#a-quick-example
# Ref: https://github.com/apache/spark/blob/master/docs/structured-streaming-programming-guide.md
# Simple Spark streaming. Output is a DataFrame
#*****************
# Use spark-3.0.0
#*****************

#
# Objective:
#		1. Word count: Count the number of words in a sentence
#		   coming over the network--Use spark structured streaming
#                  Output is in a DataFrame format

# Usage:
#    Steps:
#		     1.  Start hadoop
#		     2. On one terminal, first run this program as:
#                       (copy and paste)


# Generate fake sentences every two seconds and feed it to netcat server

cd ~
rm  -f  $HOME/Documents/results.txt
# Just have a look at the fake sentences:
python /home/ashok/Documents/fake/fake_sentence_generator.py  
# Now feed the sentences to netcat:
python /home/ashok/Documents/fake/fake_sentence_generator.py | nc -lk 9999



#
#     3. Next, on another terminal type:
#        (copy and paste--Avoid it now)
"""

cd ~
rm  -f  $HOME/Documents/results.txt
export result_file=$HOME/Documents/results.txt
spark-submit $HOME/Documents/spark/3.streaming/1.readfromnetwork/2.dataframe_network_wordcnt.py localhost 9999  >  $result_file


"""
#
#    4. Then, on another terminal, examine file, results.txt, and you will find word-counts
#        (copy and paste bash script)


"""
# AVOID IT
while true; do
         echo "Next reading of file..........."
         echo "===(ctrl+c to break)====="

	echo " "
         sleep 2
        cat  $HOME/Documents/results.txt
       sleep 3
done


"""

#
#	Follow these, instead:
#	5. You can also run complete example from:  pyspark
#          (py_spark, as per: .bashrc)
#           para 2.1 onwards
#           But, later kill the ipython (pyspark) process, as:
#
#           ps aux | grep ipython
#           kill  -9 ipython
#

# 1.0 Call libraries to create SparkContext and session

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

# 1.1 Call some functions

from pyspark.sql.functions import split, explode

# 2.0 Start spark session
#     Receive streams every 1 second
#     SEE NOTE BELOW


sc = SparkContext('local',2)
spark = SparkSession(sc )


# 2.1  TO RUN ON PYSPARK (py_spark)START HERE
#         Start reading data from socket at port 9999
#        'lines' DataFrame represents an unbounded table containing the
#         streaming text data. This table contains one column of strings
#         named “value”, and each line in the streaming text data becomes
#         a row in the table. Note, that this is not currently receiving any
#        data as we are just setting up the transformation, and have not
#        yet started it.


from pyspark.sql.functions import split, explode

# 'lines' is an unbounded table
lines = spark.readStream        \
                        .format("socket")   \
                        .option("host", "localhost")  \
                        .option("port", 9999)   \
                        .load()



"""
Debugging the streaming code:
=============================
To see what does 'lines' contain, execute the following code:

df =    lines   \
             .writeStream  \
             .option("numRows", 10)   \
             .outputMode("complete")  \
             .format("console")  \
             .start()

df.awaitTermination()



"""




# 2.2 Process read lines
#         explode:  arranges an array, one per row
#        We have used two built-in SQL functions - split and explode,
#        to split each line into multiple rows with a word each.
#        In addition, we use the function alias to name the new
#        column as “word”. 'value' is column name of 'lines' df

words = lines.select(
                     explode(
                              split(
                                    lines.value, " "
                                    )
                              )   \
                              .alias("word") )


# 2.3  Set up the query on the streaming data.
#         Group by 'word' and count
#         Finally, we have defined the wordCounts DataFrame by grouping
#         by the unique values in the Dataset and counting them. Note that this
#         is a streaming DataFrame which represents the running word counts
#         of the stream ie it will emit many values.

wordCounts =  words   \
             .groupBy("word")  \
             .count()


# 3.0 Publish on console and start streaming
#        All that is left is to actually start receiving
#        data and computing the counts. To do this,
#        we set it up to print the complete set of
#        counts (specified by outputMode("complete"))
#        to the console every time they are updated.
#        And then start the streaming computation using start().

#       After this code is executed, the streaming computation
#       will have started in the background.
#       numRows: Number of rows to print on console

pub =  wordCounts   \
             .writeStream  \
             .option("numRows", 10)   \
             .outputMode("complete")  \
             .format("console")  \
             .start()

# 3.1 Kill process on termination signal
#       The 'pub' query object is a handle to that active streaming query,
#       and we have decided to wait for the termination of the query
#       using awaitTermination() to prevent the process from exiting
#       while the query is active.

pub.awaitTermination()

################  FINISH ##########################
# Both sparkcontext and sparksession can be merged, as:
# Ref: https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#quick-example
#spark = SparkSession \
#    .builder \
#    .appName("StructuredNetworkWordCount") \
#    .getOrCreate()
#    Try it afterwards
##################################################

# 4.0 Simple demo of split, explode and alias
# https://stackoverflow.com/questions/38210507/explode-in-pyspark

# 4.1
from pyspark.sql.functions import split, explode
# 4.2

df = spark.createDataFrame(
                             [                                        # A list of tuples
                               ('cat elephant rat rat cat', ) ,       # Each tuple is one row
                               ('dog fly cycle fly dog', )
                             ],            # data
                             ['word']     # Column name is 'word'
                           )

"""
Try t ocreate 2-columned DataFrame:

df1 = spark.createDataFrame(
                             [
                               ('cat elephant rat rat cat',3 ) ,
                               ('dog fly cycle fly dog',4 )
                             ],            # data
                             ['word', 'x']     # Column name is 'word'
                           )

df1.show()

"""

# 4.3

df.show(truncate = False)

# 4.4

df.select(
           split(df['word'], ' ')
          ).show(truncate = False)             # Note the column name

# 4.5

df.select(
           split(df['word'], ' ').alias('word')   # Splits a text-row into a list of words
          ).show(truncate = False)

# 4.6 Split as also explode

df.select(
           explode(
                     split(df['word'], ' ')
                   ).alias('word')
           ).show(truncate = False)

# 4.7

df = df.select(
                explode(
                         split(df['word'], ' ')
                        ).alias('word')
               )

df.groupBy("word").count().show()


###############
# Quick code to understand complete example using mix of RDD
# and Structred Spark DataFrame.
# We use just one DStream

# Q1.0

from pyspark.sql import Row
from pyspark.sql.functions import split, explode

# Q2.0 Just one DStream

lines = ["Good morning. Nice day", "OK bye bye", "Good work", "Good day"]

# Q3.0 Transform to RDD and apply to each element
#             of list, function Row()

lines = sc.parallelize(lines).map (lambda x: Row(x))
lines.count()


# Q4.0 Convert it to dataframe with column name as 'value'

lines = sqlContext.createDataFrame(lines, ['value'])
lines.collect()
lines.show(truncate = False)

# Q5.0  What do split and explode do?
#             explode: Returns a new row for each element in the given array

lines.select( split(lines.value, " ")) .show(truncate = False)
lines.select( explode( split(lines.value, " ") )).show(truncate = False)
lines.select( explode( split(lines.value, " ") ).alias("word") ).show(truncate = False)

# Q6.0
words = lines.select( explode( split(lines.value, " ") ).alias("word") )
wordCounts = words.groupBy("word").count()
wordCounts.collect()

###########################

# Last amended: 15th April, 2023
# My folder: /home/ashok/Documents/spark/3.streaming/1.readfromnetwork
#  VM: lubuntu_spark

"""
Ref: https://github.com/apache/spark/blob/v2.3.2/examples/src/main/python/sql/streaming/structured_network_wordcount_windowed.py
Re:  https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#window-operations-on-event-time
Re: Structured Streaming API
      https://spark.apache.org/docs/latest/api/python/reference/pyspark.ss.html

 Counts words in UTF8 encoded, '\n' delimited text received from the network over a
 sliding window of configurable duration. Each line from the network is tagged
 with a timestamp that is used to determine the windows into which it falls.

 Objective: Count words within <window duration> window, updating every <slide duration>

     <window duration>:  Gives the size of window, specified as integer number of seconds
     <slide duration>:   Gives the amount of time successive windows are offset from one another,
                         given in the same units as above. <slide duration> should be less than
                         or equal to <window duration>
     <window duration>.  If the two are equal, successive windows have no overlap. If
                         <slide duration> is not provided, it defaults to <window duration>.



# Usage:
#    Steps:
#		     1.  Start hadoop
#		      2. On one terminal, first run this program as:

cd ~
rm  -f  $HOME/Documents/results.txt
python /home/ashok/Documents/fake/fake_sentence_generator.py | nc -lk 9999

#                  3. Next, on another terminal type
##                    execute the following:
##


"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import window


# 1.0 Our window and slide durations:
windowDuration = "30 seconds"
slideDuration = "20 seconds"

# 2.0 Get/create Spark Session
spark = SparkSession\
                    .builder\
                    .appName("StructuredNetworkWordCountWindowed")\
                    .getOrCreate()

# 3.0 Create DataFrame representing the stream of input lines
#     from connection to host:port. Stream includes time-stamps

lines = spark\
               .readStream\
               .format('socket')\
               .option('host', 'localhost')\
               .option('port', 9999)\
               .option('includeTimestamp', 'true')\
               .load()


"""
Debugging conside
==================
To have a look at timestamps, execute the following code:


df      =        lines\
                     .writeStream\
                     .outputMode('update')\
                     .option("numRows", 40)   \
                     .format('console')\
                     .option('truncate', 'false')\
                     .start()



df.awaitTermination()

"""





# 3.1 Split the lines into words, retaining timestamps
#     split() splits each line into an array, and explode() turns the array into multiple rows:

words = lines.select(
                     explode(split(lines.value, ' ')).alias('word'),
                     lines.timestamp
                    )

# 3.2 Group the data, group by window and by word and
#       compute the count of each group


"""

10 minute windows, updating every 5 minutes:
    That is, word counts in words received between 10 minute windows 12:00 - 12:10,
    12:05 - 12:15, 12:10 - 12:20, etc. Note that 12:00 - 12:10 means data that arrived
    after 12:00 but before 12:10. Now, consider a word that was received at 12:07.
    This word should increment the counts corresponding to two windows 12:00 - 12:10
    and 12:05 - 12:15. So the counts will be indexed by both, the grouping key
    (i.e. the word) and the window (can be calculated from the event-time).

"""

"""
groupby window will effectively create a new column, as:

Existing data:

event          time
yes            12:01
yes            12:03
yes            12:09
yes            12:11
yes            12:13
yes            12:19
yes            12:21

After groupby

event		time              window1	window2
yes		12:01		12:00--12:10
yes		12:03		12:00--12:10	
yes		12:09		12:00--12:10	12:05--12:15
yes		12:11		12:10--12:20	12:05--12:15
yes		12:13		12:10--12:20	12:05--12:15
yes		12:21		12:20--12:30    		

"""



windowedCounts = words.groupBy(
                                   window(
                                           words.timestamp,
                                           windowDuration,    # Count every windowDuration
                                           slideDuration      # Update every slideDuration
                                          ),
                              words.word
                             ).count().orderBy('window')

# 3.3 Start running the query that prints the windowed
#      word counts to the console


query = windowedCounts\
                     .writeStream\
                     .outputMode('complete')\
                     .option("numRows", 40)   \
                     .format('console')\
                     .option('truncate', 'false')\
                     .start()



query.awaitTermination(timeout = 180)  # Terminate after 180 seconds
                                      # Else, kill as:
                                      #    pgrep ipython  => To get pid
                                      #    kill -9 <pid>
                                      #    ps aux | grep ipython
                                      #  (the first line gives pid)




#### All code at  one place ####################


from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import window

windowDuration = "30 seconds"
slideDuration = "20 seconds"

lines = spark\
               .readStream\
               .format('socket')\
               .option('host', 'localhost')\
               .option('port', 9999)\
               .option('includeTimestamp', 'true')\
               .load()


words = lines.select(
                     explode(split(lines.value, ' ')).alias('word'),
                     lines.timestamp
                    )

windowedCounts = words.groupBy(
                                   window(
                                           words.timestamp,
                                           windowDuration,    # Count every windowDuration
                                           slideDuration      # Update every slideDuration
                                          ),
                              words.word
                             ).count().orderBy('window')


query = windowedCounts\
                     .writeStream\
                     .outputMode('complete')\
                     .option("numRows", 40)   \
                     .format('console')\
                     .option('truncate', 'false')\
                     .start()



query.awaitTermination()



############# update mode ##########

# Terminate above query and paste the following three
#  commands.
#   sorting is NOT supported in 'update' mode.


from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import window

windowDuration = "30 seconds"
slideDuration = "20 seconds"

lines = spark\
               .readStream\
               .format('socket')\
               .option('host', 'localhost')\
               .option('port', 9999)\
               .option('includeTimestamp', 'true')\
               .load()


words = lines.select(
                     explode(split(lines.value, ' ')).alias('word'),
                     lines.timestamp
                    )

windowedCounts = words.groupBy(
                                   window(
                                           words.timestamp,
                                           windowDuration,    # Count every windowDuration
                                           slideDuration      # Update every slideDuration
                                          ),
                              words.word
                             ).count()



query = windowedCounts\
                     .writeStream\
                     .outputMode('update')\
                     .option("numRows", 40)   \
                     .format('console')\
                     .option('truncate', 'false')\
                     .start()



query.awaitTermination(timeout = 180)   # Terminate after 100 seconds   

#######################################################

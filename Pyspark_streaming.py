# Last amended: 4th Dec, 2021s
# Ref: https://spark.apache.org/docs/latest/streaming-programming-guide.html#a-quick-example
# My folder: /home/ashok/Documents/spark/streaming/1.readfromnetwork
# Simple Spark streaming
#VM: lubuntu_spark.vdi
#*****************
# Use spark-3.0.0
#*****************
# Objectives:
#		1. Word count: Count the number of words in a sentence
#		   coming over the network
#               2. Run first Quick code below to understand
#
# Usage:
#		     1. Start hadoop
#		        On one terminal, run this program as:
#
#  cd ~
#    $ spark-submit  /home/ashok/Documents/spark/3.streaming/1.readfromnetwork/1.RDD_network_wordcnt.py  localhost 9999  >  /home/ashok/Documents/results.txt
#
#                2. On another terminal start generating fake-sentences at 2-secs interval
#                   and feed them to netcat server to output at port 9999

"""

python /home/ashok/Documents/fake/fake_sentence_generator.py | nc -lk 9999

"""
#
#	Then printout file,  results.txt, and you will find word-counts
#
#            ELSE:
#		3. You can also run complete example from:  pyspark (ipython)
#                  But, later kill the ipython (pyspark) process, as:
#
#                      ps aux | grep ipython
#                      kill  -9 ipython
#
"""
Broad Steps:

After a streaming context is defined, one has to do the following.

    i)    Create a streaming context
    ii)   Create an object using streaming context that will
           connect to input sources and output Dstreams
   iii)   Use the object to read streams
   iv)   Write the streaming computations by applying transformation
           and output operations to DStreams.
    iii)  Start receiving data and processing it using
           streamingContext.start().
    iv)  Stop processing using streamingContext.awaitTermination().
    v)   The processing can be manually stopped using streamingContext.stop().

"""


##################

# 1.0 Call libraries
from pyspark import SparkContext     			# Not needed in pyspark

# 2.0 Create a local StreamingContext with 2 working threads and
#         batch interval of 5 seconds

sc = SparkContext(
                  "local[2]",            # [2] refers to 2 threads
                                         # 'local[*]' means detect number
                                         #  of cores yourself
                                         # DO NOT MAKE IT 1
                  "NetworkWordCount"     # App name that will be shown
                                         # in spark UI
                  )                      # Not needed in pyspark


# 2.1  To run on pyspark, please start here

from pyspark.streaming import StreamingContext

# 2.1.1 Create a Streaming context
#       A streaming context provides method to create
#       DStreams from various input sources (kafka, flume, TCP sockets, files etc)
#       '5' refers to 'batch' interval of DStream in seconds
#
ssc = StreamingContext(sc, 5)   # If it is 120, streaming output will be
                                #  available in batches of 2minutes

# 3.0 Create a DStream object that will connect to TCP source
#     as hostname:port, like localhost:9999. The complete input
#     whether on one line  or two lines or multiple lines is read as
#     one string in a give time interval:
#     (For other types of stream readers, see:
#      https://spark.apache.org/docs/0.7.2/api/streaming/spark/streaming/StreamingContext.html )

lines = ssc.socketTextStream("localhost", 9999)

# 4. Split each string into words.
#      flatMap creates another DStream
#      Basically all lines read in a given interval
#      are being split into words, and put in one list.
#      Note that it is not that each line constitutes
#      a list of words. All lines together will create
#     just one list
"""
flatmap: map followed by flattening
The values from the stream returned by the mapper are drained
from the stream and are passed to the output stream. The "clumps"
of values returned by each call to the mapper function are not
distinguished at all in the output stream, thus the output is
said to have been "flattened".
https://stackoverflow.com/a/26684710
A structure like [ [1,2,3],[4,5,6],[7,8,9] ] which has "two levels".
Flattening this means transforming it in a "one level"
structure : [ 1,2,3,4,5,6,7,8,9 ].

"""

words = lines.flatMap(lambda line: line.split(" "))

# 5. For every word, create a pair (word,1).

pairs = words.map(lambda word: (word, 1))

# 6. For every pair, add values:
#    x: accumulator for each key
#    y: value against each key

wordCounts = pairs.reduceByKey(lambda x, y: x + y)

# 7. Print the first ten elements of each RDD generated
#     in this DStream to the console

wordCounts.pprint()

# 8.0   Start the process:

ssc.start()             			  # Start the computation
ssc.awaitTermination()	  # Wait for the computation to terminate



##########################################

# Quick code
#  Shows step-by-step implementation of code above
#  Instead of multiple DStream 'clumps', we use just one DStream

# Q1.0 Lines is our 1 DStream object.
lines = ["Good morning. Nice day", "OK bye bye", "Good work", "Good day", "Good day"]

# Q2.0 parallelize() operation trasfroms the list to RDDs
#      distributed across machines in a cluster

lines = sc.parallelize(lines)      # Convert lines to RDD

# Q2.1 Some operation on lines object:

lines.collect()
lines.count()              # 5 No of elements within RDD
lines.distinct().count()   # 4 No of distinct elements within RDD
lines.countApprox(2)       # Quickly count in 2 secs

# Q3.0
# https://data-flair.training/blogs/apache-spark-map-vs-flatmap/

abc   = lines.map(lambda line: line.split(" "))       # Split each line at space
abc.collect()                                         # [['Good','morning.','Nice','day'], ['OK','bye','bye']]

words = lines.flatMap(lambda line: line.split(" "))    # Split each line on space and flatten inner lists
words.collect()        			               # ['Good', 'morning.', 'Nice', 'day', 'OK', 'bye', 'bye']


# Q4.0 For each word, create a tuple as,
#      ('Good',1)

pairs = words.map(lambda word: (word, 1))
pairs.collect()      			    # A list of tuples: [('Good', 1), ('morning.', 1),...]

# Q5.0 Sum up values (y), one by one, and accumulate in accumlator (x)
# Ref: https://stackoverflow.com/questions/30145329/reducebykey-how-does-it-work-internally
#   wordCounts pairs.reduceByKey((accumulatedValue: Int, currentValue: Int) => accumulatedValue + currentValue)
# Key is the word. For each key, say 'Good', value (y) is summed
# up with the already accumulated value (x)
# That is there are as many accumulators as there
# are keys (so to say):

wordCounts = pairs.reduceByKey(lambda x, y: x + y)
wordCounts.collect()        # [('bye', 2), ('morning.', 1),

#######################################################

# Last amended: 3rd April, 2023
# Objective:
#            a. As invoice files are generated, one by one. analyse them
#            b. Perform time-window based analysis
#
# Refer: https://www.macrometa.com/event-stream-processing/spark-structured-streaming
# Part 4 Databricks:  https://www.databricks.com/blog/2017/05/08/event-time-aggregation-watermarking-apache-sparks-structured-streaming.html


"""
Usage:

	Step 1: Start hadoop
	Step 2: Follow steps (a) and (b) of file:
		 file '/home/ashok/Documents/fake/fake_invoice_gen_ver2.py
	Step 3: Start py_spark in another terminal
	Step 4: In py_spark %paste this complete program
	Step 5: In earlier terminal :
                c. Start generating fake invoice files
	Step 6: Watch output in py_spark
"""

# Open pyspark with ipython interface, as:

#             py_spark


# 1.0 Import necessary library
from pyspark.sql.types import  StructType, IntegerType, DateType, StringType

# 1.1 Define schema of data:
invoiceSchema = StructType() \
                            .add("cusid", IntegerType(), True) \
                            .add("item", StringType(), True) \
                            .add("price", IntegerType(), True) \
                            .add("qty", IntegerType(), True) \
                            .add("eventTime", DateType(), True)

"""
Another schema def method:

  StructType()  \
   .add("cusid", "int")  \
   .add("item", "long")  \
   .add("price", "int")

Not sure about datetime

"""



# 1.2 Read stream of data:
#     You can read files either from Linux
#       or from hadoop:

linuxDir        =  "file:///home/ashok/Documents/spark/data"
hadoopDir       =  "hdfs://localhost:9000/user/ashok/spark/data"

# 1.2.1
dfClients =  spark \
		 .readStream \
		 .format("csv") \
		 .option("header",True) \
		 .option("path",hadoopDir) \
		 .schema(invoiceSchema) \
		 .load()


# 1.2.2 Interested in only those who purchase chilli powder:
chilliCustomers = dfClients.select("cusid","qty").where("item = 'chilli' ")



# 2.0 Write output to console:
chilliCustomers \
		 .writeStream \
                 .outputMode("append") \
		 .format("console") \
		 .start()




#######################################


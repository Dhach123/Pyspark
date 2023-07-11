#!/bin/bash


# Last amended: 12th August, 2020
# Myfolder: /home/ashok/Documents/spark/3.streaming/2.streamfromfiles
# VM:  lubuntu_spark

## Objective:
# 		i)   Generates multiple files
# 		ii)  Generates fake data files
#             iii) Files are then copied to hadoop
# 		      For use in spark streaming

# 		Press ctrl+c to terminate for-loop


## About fake data:
# 	Fake data has two fields: string and integer 
#  	Separator is semicolon. There is no header.
#     Each file has 499 records. Many strings may
#     repeat, as here:
"""
Lib;28
Tim;30
Mal;28
Mal;41
Bru;28
Geo;45
Dom;59

"""


## Usage:
#   Open one terminal and run this file as:
#     	cd  $HOME/Documents/spark/3.streaming/2.streamfromfiles
#             chmod  +x   file_gen.sh
#    		./file_gen.sh
#
#      Leave that terminal as it is

# 0.0 If 'data' folder does not exist, create it

if [ ! -d $HOME/Documents/spark/data ]; then
  mkdir -p $HOME/Documents/spark/data;
fi



# 1.0 First delete any existing file in the folder
echo " "
echo " "
echo "Deleting existing files from folder:   /home/ashok/Documents/spark/data "
echo "   "
cd  $HOME/Documents/spark/data
rm -f *.csv
echo "Done...."

echo "Deleting existing  folder from hadoop"
echo "hdfs://localhost:9000/user/ashok/data_files/fake"
hdfs dfs -rm -r -f  hdfs://localhost:9000/user/ashok/data_files/fake
echo "Done..."

# 1.1 This folder will contain generated datafiles
echo "Creating folder in hadoop to contain fake files"
echo "hdfs://localhost:9000/user/ashok/data_files/fake"
hdfs dfs -mkdir   -p  hdfs://localhost:9000/user/ashok/data_files/fake
echo "Done..."
echo "  "

cd ~
# 2.0 Generate files at 10 seconds interval
for i in {1..100}
do
   echo "Creating  file $i.csv . "
   python  /home/ashok/Documents/fake/simpleDataGen.py  >  /home/ashok/Documents/spark/data/$i.csv
   echo "Copying file $i.csv  to hadoop "
   hdfs dfs -put  /home/ashok/Documents/spark/data/$i.csv  /user/ashok/data_files/fake
   echo "Will create next fake file after 10 seconds"
   echo "----------------------------------------------------------"
   sleep 10
done

################# END ###########################
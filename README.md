# Log-Generator-Map-Reduce

In this project, we will explore the Hadoop File System and execute MapReduce programs in it - first on out local machine and then on AWS EMR clusters.


As a starting point, we have been given a code snippet to generate custom log files of a specified format along with an application configuration to tune our log file generator output. The program also injects a random regular expression string into the log message text based on a predefined probability. In general, a log file generated by this code looks like this:

12:00:03.551 [main] INFO  GenerateLogData$ - Log data generator started...
12:00:03.888 [main] WARN  HelperUtils.Parameters$ - Max count 500 is used to create records instead of timeouts
12:00:04.036 [scala-execution-context-global-14] INFO  HelperUtils.Parameters$ - 7b*hK9kl_5v,#_R1L*B0S9fN6gM6pcg1ae3cg1ag2bg2H6lce0crtRa>D\}=`.K7+^qk<R
12:00:04.073 [scala-execution-context-global-14] ERROR HelperUtils.Parameters$ - Mai<Fy{9yag2U8scf2T5kbe1T5iI5oR7fcg1bf3D$=KEmoCF
12:00:04.096 [scala-execution-context-global-14] ERROR HelperUtils.Parameters$ - o5R=aJn'i%(77"I[R_o_K7rN5pag1T5hG6hC#&srJDINCRE3D&gbr{8
12:00:04.122 [scala-execution-context-global-14] ERROR HelperUtils.Parameters$ - yU}4lSb%uLxk!o:HTySX1{dU}E:G2-F!1dCV+lVe@XIiSP5b
12:00:04.154 [scala-execution-context-global-14] INFO  HelperUtils.Parameters$ - p/4NMdwQdg9XAmgO\9"b$aKI"o'L(KM|j2_J1CK(&T8&AN_N7}Vy9C7L
.
.
.
12:00:11.872 [main] INFO  GenerateLogData$ - Log data generation has completed after generating 500 records.
We will generate many such logs and run MapReduce algorithms on them in order to calculate the following metrics:

Distribution of different types of logs across predefined time intervals for logs with the injected regex
Time intervals with most ERROR messages sorted in descending order for logs with the injected regex
Number of log messages for each type of log message: INFO, DEBUG, WARN, ERROR
Number of characters in each log message type having the highest number of characters for logs with the injected regex
Prerequisites & Installation
In order to run the algorithms implemented in this project, I recommend cloning this repository onto your local machine and running it from the command-line using the interactive build tool sbt.

Note: In order to install sbt, please follow the OS-specific instructions at https://www.scala-sbt.org/1.x/docs/Setup.html.

To clone the repo use:

git clone https://https://github.com/Adeemshaik/Log-Generator-Map-Reduce.git
Generate a log file through the provided log file generator (invoke the main method, no system arguments required): GenerateLogData.scala

The log file will be generated in the log/ folder at the project root.

Next, navigate to the repo and use the following command to create an executable JAR file:

sbt clean; sbt assembly
The generated JAR file will be present in the target/scala3.x.y/ folder at the root of the project directory.

To run the unit tests (which might be ideally done before running the algorithms themselves), use the following command:

sbt clean; sbt test
In order to run the entire suite of steps, use the following command:

sbt clean; sbt test; sbt assembly
In order to run the MapReduce algorithms after generating the JAR file, follow the next steps.

Running the MapReduce Algorithms
There are 3 options to run the provided MapReduce algorithms:

Run on your local machine
Run on a ready-made Hadoop sandbox such as Cloudera's Hortonworks
Run on an AWS EMR cluster
Run on your local machine
Download and install Apache Hadoop on your system:
Single-node setup: https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html
Multi-node setup: https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/ClusterSetup.html
Start the Hadoop cluster
On the Hadoop File System (HDFS), create a folder named input/ and copy our generated log file to it:
hadoop fs -mkdir -p input/
hadoop fs -put /path/to/local/log/file input/LogFileGenerator.log
Run the JAR file on the Hadoop cluster with our previously copied log file as input:
hadoop jar /path/to/local/jar/file input/LogFileGenerator.txt output/
Once the execution finishes, you can view the MapReduce output on the HDFS via:
hadoop fs -cat output/NameOfMapReduceAlgorithm/part-r-00000
There could be many such "part" files for each algorithm, so please be sure to check them all
You can use hadoop fs -ls hdfs/directory to see the contents in any HDFS path
In order to combine all the output files, use:
hadoop fs -getMerge output/NameOfMapReduceAlgorithm/ path/to/local/output/file.txt
Delete the HDFS output before re-running the algorithms:
hadoop fs -rm -r -f output/
Run on a ready-made Hadoop sandbox
Dr. Mark has already provided the instructions on how to obtain a VMware license and download the Hortonworks VM. For further reference, please refer to this link: https://hortonworks.com/wp-content/uploads/2012/03/Tutorial_Hadoop_HDFS_MapReduce.pdf

Run on an AWS EMR cluster
As mentioned before, the steps to running the JAR file on AWS EMR can be found here.

MapReduce Tasks In-depth
Now let us take a look at each implemented MapReduce task one by one.

Task 1: Distribution of different types of logs across predefined time intervals for logs with the injected regex
In this scenario, our objective is to parse through all the logs one by one and find their distribution over each unique time interval. However, we are supposed to do this only for the logs that have the injected regex present in them.

For example, if this is our log file:

12:00:03.551 [main] INFO  GenerateLogData$ - Log data generator started...
12:00:03.888 [main] WARN  HelperUtils.Parameters$ - Max count 500 is used to create records instead of timeouts
12:00:04.036 [scala-execution-context-global-14] INFO  HelperUtils.Parameters$ - 7b*hK9kl_5v,#_R1L*B0S9fN6gM6pcg1ae3cg1ag2bg2H6lce0crtRa>D\}=`.K7+^qk<R
12:00:04.073 [scala-execution-context-global-14] WARN HelperUtils.Parameters$ - Mai<Fy{9yag2U8scf2T5kbe1T5iI5oR7fcg1bf3D$=KEmoCF
12:00:04.096 [scala-execution-context-global-14] DEBUG HelperUtils.Parameters$ - o5R=aJn'i%(77"I[R_o_K7rN5pag1T5hG6hC#&srJDINCRE3D&gbr{8
12:00:04.122 [scala-execution-context-global-14] ERROR HelperUtils.Parameters$ - yU}4lSb%uLxk!o:HTySX1{dU}E:G2-F!1dCV+lVe@XIiSP5b
12:00:04.154 [scala-execution-context-global-14] INFO  HelperUtils.Parameters$ - p/4NMdwQdg9XAmgO\9"b$aKI"o'L(KM|j2_J1CK(&T8&AN_N7}Vy9C7L
12:00:11.872 [main] INFO  GenerateLogData$ - Log data generation has completed after generating 500 records.
Our output should look like this:

INFO_12:00:4	2
DEBUG_12:00:4	1
WARN_12:00:4	1
ERROR_12:00:4	1
Basically, we identify a unique time interval for each log type and count the amount of log messages of that type that fall into that interval. Here, we have divided our time intervals based on each second. We are rounding off to the nearest second in our code in order to fit the logs into their respective intervals.

Mapper
For this task, our mapper performs the following actions:

Read the log message
Extract the timestamp and log message type
Create a key in the format: logtype_timestamp
Check whether the log message text matches the given regex pattern
If yes, write the key to the HDFS with value 1
If no, do nothing
The logic here is that for all the keys that fall under the same time interval, we differentiate among the messages by appending the timestamp to each type of message and marking its occurrence with a value 1. In the reducer we will sum up these "1" values to get an exact count.

Reducer
For this task, our reducer performs the following actions:

Read the mapper output as input
For each set of unique keys, sum up their "1" values to get the total count
Write the key and its count to the HDFS
Sample output
Task 1 sample output here.

DEBUG_12:00:12	3
DEBUG_12:00:6	4
DEBUG_12:00:9	5
ERROR_12:00:10	39
ERROR_12:00:5	33
.
.
.
Task 2: Time intervals with most ERROR messages sorted in descending order for logs with the injected regex
In this scenario, our objective is to parse through all the logs one by one and find the time interval with most ERROR messages and sort these time intervaks in descending order. However, we are supposed to do this only for the logs that have the injected regex present in them.

For example, if this is our log file:

12:00:03.551 [main] INFO  GenerateLogData$ - Log data generator started...
12:00:03.888 [main] WARN  HelperUtils.Parameters$ - Max count 500 is used to create records instead of timeouts
12:00:04.036 [scala-execution-context-global-14] ERROR  HelperUtils.Parameters$ - 7b*hK9kl_5v,#_R1L*B0S9fN6gM6pcg1ae3cg1ag2bg2H6lce0crtRa>D\}=`.K7+^qk<R
12:00:04.073 [scala-execution-context-global-14] WARN HelperUtils.Parameters$ - Mai<Fy{9yag2U8scf2T5kbe1T5iI5oR7fcg1bf3D$=KEmoCF
12:00:04.096 [scala-execution-context-global-14] DEBUG HelperUtils.Parameters$ - o5R=aJn'i%(77"I[R_o_K7rN5pag1T5hG6hC#&srJDINCRE3D&gbr{8
12:00:04.122 [scala-execution-context-global-14] ERROR HelperUtils.Parameters$ - yU}4lSb%uLxk!o:HTySX1{dU}E:G2-F!1dCV+lVe@XIiSP5b
12:00:05.154 [scala-execution-context-global-14] ERROR  HelperUtils.Parameters$ - p/4NMdwQdg9XAmgO\9"b$aKI"o'L(KM|j2_J1CK(&T8&AN_N7}Vy9C7L
12:00:11.872 [main] INFO  GenerateLogData$ - Log data generation has completed after generating 500 records.
Our output should look like this:

12:00:5	1
12:00:4	2
Basically, we only parse those logs that contain an ERROR log type and find out their timestamp. Next, we sort this data based on the timestamp.

Mapper
For this task, our mapper performs the following actions:

Read the log message
Extract the timestamp and log message type
For each log message of type ERROR:
Create a key in the format: timestamp
Check whether the log message text matches the given regex pattern
If yes, write the key to the HDFS with value 1
If no, do nothing
The logic here is that for each ERROR log that falls under any time interval, we mark its occurrence with a value 1. In the reducer we will sum up these "1" values to get an exact count and then sort the data by timestamps.

From the Apache Hadoop documentation, we can see that at any mapping or reducing step, data is automatically sorted by key. However, this sorting is in ascending order. In order to reverse the order of sorting, we will be converting every timestamp into a text-based format so the reducer sorts the keys in alphabetical order and not a numerical order. In the case of timestamps, this automatically gives a descending sort.

Reducer
For this task, our reducer performs the following actions:

Read the mapper output as input
For each unique key type, sum up all the "1" values to get the total count
Write the key and its count to the HDFS
Sample output
Task 2 sample output here.

12:00:11 25
12:00:4	 6
12:00:7	 33
.
.
.
Task 3: Number of log messages for each type of log message: INFO, DEBUG, WARN, ERROR
In this scenario, our objective is to parse through all the logs one by one and find count the number of logs for each log type.

Note: This scenario is analogous to the WordCount example shared by Dr. Mark on Teams. As a result, there is some overlap between the WordCount example and this algorithm.

For example, if this is our log file:

12:00:03.551 [main] INFO  GenerateLogData$ - Log data generator started...
12:00:03.888 [main] WARN  HelperUtils.Parameters$ - Max count 500 is used to create records instead of timeouts
12:00:04.036 [scala-execution-context-global-14] INFO  HelperUtils.Parameters$ - 7b*hK9kl_5v,#_R1L*B0S9fN6gM6pcg1ae3cg1ag2bg2H6lce0crtRa>D\}=`.K7+^qk<R
12:00:04.073 [scala-execution-context-global-14] WARN HelperUtils.Parameters$ - Mai<Fy{9yag2U8scf2T5kbe1T5iI5oR7fcg1bf3D$=KEmoCF
12:00:04.096 [scala-execution-context-global-14] DEBUG HelperUtils.Parameters$ - o5R=aJn'i%(77"I[R_o_K7rN5pag1T5hG6hC#&srJDINCRE3D&gbr{8
12:00:04.122 [scala-execution-context-global-14] ERROR HelperUtils.Parameters$ - yU}4lSb%uLxk!o:HTySX1{dU}E:G2-F!1dCV+lVe@XIiSP5b
12:00:05.154 [scala-execution-context-global-14] ERROR  HelperUtils.Parameters$ - p/4NMdwQdg9XAmgO\9"b$aKI"o'L(KM|j2_J1CK(&T8&AN_N7}Vy9C7L
12:00:11.872 [main] INFO  GenerateLogData$ - Log data generation has completed after generating 500 records.
Our output should look like this:

INFO  1
DEBUG 1
WARN  1
ERROR 2
Mapper
For this task, our mapper performs the following actions:

Read the log message
Extract the log message type
Create a key in the format: logtype
Write the key to the HDFS with value 1
In the reducer we will sum up these "1" values to get an exact count of each log type.

Reducer
For this task, our reducer performs the following actions:

Read the mapper output as input
For each unique key type, sum up all the "1" values to get the total count
Write the key and its count to the HDFS
Sample output
Task 3 sample output here.

ERROR 150
DEBUG 761
INFO  1023
WARN  206
Task 4: Number of characters in each log message type having the highest number of characters for logs with the injected regex
In this scenario, our objective is to parse through all the logs one by one and find the length of each log string. For each type of log message, our goal is to find the string with the maximum length. However, we are supposed to do this only for the logs that have the injected regex present in them.

For example, if this is our log file:

12:00:03.551 [main] INFO  GenerateLogData$ - Log data generator started...
12:00:03.888 [main] WARN  HelperUtils.Parameters$ - Max count 500 is used to create records instead of timeouts
12:00:04.036 [scala-execution-context-global-14] ERROR  HelperUtils.Parameters$ - 7b*hK9kl_5v,#_R1L*B0S9fN6gM6pcg1ae3cg1ag2bg2H6lce0crtRa>D\}=`.K7+^qk<R
12:00:04.073 [scala-execution-context-global-14] WARN HelperUtils.Parameters$ - Mai<Fy{9yag2U8scf2T5kbe1T5iI5oR7fcg1bf3D$=KEmoCF
12:00:04.096 [scala-execution-context-global-14] DEBUG HelperUtils.Parameters$ - o5R=aJn'i%(77"I[R_o_K7rN5pag1T5hG6hC#&srJDINCRE3D&gbr{8
12:00:04.122 [scala-execution-context-global-14] ERROR HelperUtils.Parameters$ - yU}4lSb%uLxk!o:HTySX1{dU}E:G2-F!1dCV+lVe@XIiSP5b
12:00:05.154 [scala-execution-context-global-14] INFO  HelperUtils.Parameters$ - p/4NMdwQdg9XAmgO\9"b$aKI"o'L(KM|j2_J1CK(&T8&AN_N7}Vy9C7L
12:00:11.872 [main] INFO  GenerateLogData$ - Log data generation has completed after generating 500 records.
Our output should look like this:

INFO  56
DEBUG 55
WARN  48
ERROR 70
Basically, find the length (number of characters) in each log message that matches the regex. Next, we sort this data for each log type in descending order and pick the length on top of the sorted list.

Mapper
For this task, our mapper performs the following actions:

Read the log message
Extract the log message type
Create a key in the format: logtype
Extract the log string and get its length
For each log message that matches the given regex pattern:
Write the key to the HDFS with length as value
The logic here is that we rely on our reducer to sort the mapper output by the value length and pick the max for each log type.

Reducer
For this task, our reducer performs the following actions:

Read the mapper output as input
For each unique key type, get all the character lengths
Sort these lengths for each key and pick the max
Write the key and its max to the HDFS
Sample output
Task 4 sample output here.


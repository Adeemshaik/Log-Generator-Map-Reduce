package MapReduce

// Importing libraries
import HelperUtils.{CreateLogger, ObtainConfigReference}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.slf4j.Logger

import scala.collection.JavaConverters.*
import java.lang

object NumberOfMessagesForEachLogType:

  // Initializing configuration
  val config = ObtainConfigReference("randomLogGenerator") match {
    case Some(value) => value
    case None => throw new RuntimeException("Cannot obtain a reference to the config data.")
  }

  // This is our mapper class
  class TokenizerMapper extends Mapper[Object, Text, Text, IntWritable] {

    // Initializing the key (word), value(one) and logger instances
    val one = new IntWritable(1)
    val word = new Text()
    val logger: Logger = CreateLogger(classOf[TokenizerMapper])

    // This is our mapper
    // It takes the log file input line by line and creates key value pairs of log type -> count
    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {

      // Getting the log message as a single string and splitting it into units
      val logMessage: Array[String] = value.toString.split(" ")
      logger.debug("Log message to be processed: " + logMessage.toList.toString)

      // Getting the type of log message - INFO, DEBUG, WARN, ERROR
      val logType = logMessage(2)
      logger.debug("Type of log message: " + logType.toString)

      // Creating the key for our mapper - logtype
      val key = logType
      logger.debug("Key created by mapper: " + key.toString)
      word.set(key)

      // Writing the key value pair as mapper output
      context.write(word, one)
    }
  }

  // This is our reducer class
  class IntSumReader extends Reducer[Text, IntWritable, Text, IntWritable] {

    // Initializing logger
    val logger: Logger = CreateLogger(classOf[IntSumReader])

    // This is our reducer
    // It takes the mapper's output and counts the number of log messages of a certain type
    override def reduce(key: Text, values: lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {

      // Counting the total number of log messages by type
      val sum = values.asScala.foldLeft(0)(_ + _.get)
      logger.debug("Sum calculated by reducer for key " + key.toString + ": " + sum.toString)

      // Writing the key and sum as reducer output
      context.write(key, new IntWritable(sum))
    }
  }

  // This function is our mapreduce entrypoint
  def run(args: Array[String]): Unit = {

    // Getting the task name from config
    val TASK_NAME = config.getString("NumberOfMessagesForEachLogType.TaskName")

    // Creating a new mapreduce config and creating a job instance for our task
    val configuration = new Configuration
    val job = Job.getInstance(configuration, TASK_NAME)

    // Setting our JAR file class, mapper class, combiner class, reducer class and output key/value class
    job.setJarByClass(this.getClass)
    job.setMapperClass(classOf[TokenizerMapper])
    job.setCombinerClass(classOf[IntSumReader])
    job.setReducerClass(classOf[IntSumReader])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])

    // Setting input path for our mapper and output path for our reducer
    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(args(1) + TASK_NAME))

    // Setting output format as CSV
    configuration.set("mapred.textoutputformat.separatorText", ",")

    // Executing the mapreduce task and waiting for its execution to finish
    job.waitForCompletion(true)

  }

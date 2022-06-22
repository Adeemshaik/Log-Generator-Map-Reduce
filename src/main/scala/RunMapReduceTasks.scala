import MapReduce.*

// This is the entrypoint to our entire JAR executable
object JobDriver {

  // This is our main function
  def main(args: Array[String]) = {

    // Calling each mapreduce task one by one
    NumberOfMessagesForEachLogType.run(args)
    DistributionOfLogsAcrossTimeIntervals.run(args)
    TimeIntervalsWithMostErrorMessagesSorted.run(args)
    HighestNumberOfCharactersForEachLogType.run(args)
  }
}
package MapReduce

// Importing libraries
import org.scalatest.funsuite.AnyFunSuite

import HelperUtils.ObtainConfigReference

class TimeIntervalsWithMostErrorMessagesSortedTest extends AnyFunSuite {

  // Initializing configuration for testing
  val config = ObtainConfigReference("Testing.TimeIntervalsWithMostErrorMessagesSorted") match {
    case Some(value) => value
    case None => throw new RuntimeException("Could not find the config data!")
  }

  // Testing divideTimeIntervals function
  test("Test: TimeIntervalsWithMostErrorMessagesSorted -> divideTimeIntervals()") {

    // Getting testing input from config
    val logTime = config.getString("Testing.TimeIntervalsWithMostErrorMessagesSorted.logTime")

    // Getting expected output from config
    val expectedLogTime = config.getString("Testing.TimeIntervalsWithMostErrorMessagesSorted.expectedLogTime")

    // Getting output of divideTimeIntervals
    val timeInterval = TimeIntervalsWithMostErrorMessagesSorted.divideTimeIntervals(logTime)

    // Checking if output matches
    assert(timeInterval == expectedLogTime)
  }

  // Testing our mapreduce driver function
  test("Test: TimeIntervalsWithMostErrorMessagesSorted -> run()") {

    // Intercepting any throwables
    intercept[Throwable] {
      // Calling our driver
      TimeIntervalsWithMostErrorMessagesSorted.run(Array("input/", "output/"))
    }
  }
}


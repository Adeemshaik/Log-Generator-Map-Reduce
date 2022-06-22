// Importing libraries
import org.scalatest.funsuite.AnyFunSuite

class RunMapReduceTasksTest extends AnyFunSuite{

  // Testing our job driver for the JAR file
  test("Test: DistributionOfLogsAcrossTimeIntervals -> run()") {

    // Intercepting any throwables
    intercept[Throwable] {
      // Calling our main job driver
      JobDriver.main(Array("input/", "output/"))
    }
  }
}

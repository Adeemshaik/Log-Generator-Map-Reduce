package MapReduce

// Importing libraries
import org.scalatest.funsuite.AnyFunSuite

import HelperUtils.ObtainConfigReference

class NumberOfMessagesForEachLogTypeTest extends AnyFunSuite {

  // Testing our mapreduce driver function
  test("Test: NumberOfMessagesForEachLogType -> run()") {

    // Intercepting any throwables
    intercept[Throwable] {
      // Calling our driver
      NumberOfMessagesForEachLogType.run(Array("input/", "output/"))
    }
  }
}


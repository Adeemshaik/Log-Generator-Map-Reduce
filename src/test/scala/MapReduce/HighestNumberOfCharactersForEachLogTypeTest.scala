package MapReduce

// Importing libraries
import org.scalatest.funsuite.AnyFunSuite

import HelperUtils.ObtainConfigReference

class HighestNumberOfCharactersForEachLogTypeTest extends AnyFunSuite {

  // Testing our mapreduce driver function
  test("Test: HighestNumberOfCharactersForEachLogType -> run()") {

    // Intercepting any throwables
    intercept[Throwable] {
      // Calling our driver
      HighestNumberOfCharactersForEachLogType.run(Array("input/", "output/"))
    }
  }
}

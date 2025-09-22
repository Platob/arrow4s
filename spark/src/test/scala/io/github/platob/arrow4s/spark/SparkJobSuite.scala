
package io.github.platob.arrow4s.spark

import munit.FunSuite

class SparkJobSuite extends FunSuite {
  test("run returns message with sum") {
    val msg = SparkJob.run("test-app")
    assert(msg.contains("Sum=6"))
  }
}

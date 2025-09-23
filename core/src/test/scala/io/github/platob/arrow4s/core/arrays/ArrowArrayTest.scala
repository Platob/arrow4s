package io.github.platob.arrow4s.core.arrays

import munit.FunSuite

class ArrowArrayTest extends FunSuite {
  test("ArrowArray.build should create IntArray for INT type") {
    val values = Seq(1, 2, 3, 4, 5)
    val array = ArrowArray[Int](values)

    assertEquals(array, values)
  }
}

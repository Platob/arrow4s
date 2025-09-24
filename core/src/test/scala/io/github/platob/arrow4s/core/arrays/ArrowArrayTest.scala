package io.github.platob.arrow4s.core.arrays

import io.github.platob.arrow4s.core.arrays.ArrowArrayTest.UnknownType
import munit.FunSuite

class ArrowArrayTest extends FunSuite {
  test("ArrowArray.build should create BooleanArray for non-nullable BOOLEAN type") {
    val values = Seq(true, false, true, false, true)
    val array = ArrowArray(true, false, true, false, true)

    assertEquals(array, values)
  }

  test("ArrowArray.build should create BooleanArray for nullable BOOLEAN type") {
    val values = Seq(Some(true), None, Some(false), Some(true), None)
    val array = ArrowArray(Some(true), None, Some(false), Some(true), None)

    assertEquals(array, values)
  }

  test("ArrowArray.build should create IntArray for non-nullable INT type") {
    val values = Seq(1, 2, 3, 4, 5)
    val array = ArrowArray(1, 2, 3, 4, 5)

    assertEquals(array, values)
  }

  test("ArrowArray.build should throw for unsupported type") {
    val exception = intercept[IllegalArgumentException] {
      ArrowArray[UnknownType](new UnknownType())
    }
    assertEquals(exception.getMessage, "Unsupported Scala type for Arrow conversion: io.github.platob.arrow4s.core.arrays.ArrowArrayTest.UnknownType")
  }
}

object ArrowArrayTest {
  class UnknownType {}
}


package io.github.platob.arrow4s.core.arrays

import io.github.platob.arrow4s.core.arrays.primitive.IntegralArray.IntArray
import io.github.platob.arrow4s.core.values.UInt
import munit.FunSuite
import org.apache.arrow.vector.IntVector

class ArrowArrayTest extends FunSuite {
  val values: Seq[Int] = Seq(1, 2, 3, 4, 5)

  test("ArrowArray.make Long array with cast") {
    val array = ArrowArray(values:_*)

    // Assert is instance IntegralArray
    assert(array.isInstanceOf[IntArray])
    assert(array.as[Int].isInstanceOf[IntArray])
    assert(array.as[Long].isInstanceOf[LogicalArray[IntVector, Int, Long]])
    assertEquals(array.as[Long], values.map(_.toLong))
  }

  test("ArrowArray.make UInt array with cast") {
    val array = ArrowArray(values.map(UInt.trunc):_*)

    assertEquals(array, values.map(UInt.trunc))
  }

  test("ArrowArray.as should cast to optional" ) {
    val array = ArrowArray(values:_*).as[Option[Int]]

    assertEquals(array, values.map(Option.apply))
  }

  test("ArrowArray.as should cast to optional other type") {
    val values: Seq[Int] = Seq(1, 2, 3, 4, 5)
    val array = ArrowArray(values:_*)

    assertEquals(
      array.as[Option[Long]], values.map(v => Option(v.toLong))
    )
    assertEquals(
      array.as[Option[Double]], values.map(v => Option(v.toDouble))
    )
  }
}

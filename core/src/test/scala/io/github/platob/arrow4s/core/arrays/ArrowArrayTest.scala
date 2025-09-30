package io.github.platob.arrow4s.core.arrays

import io.github.platob.arrow4s.core.arrays.IntegralArray.IntArray
import io.github.platob.arrow4s.core.values.UInt
import munit.FunSuite

class ArrowArrayTest extends FunSuite {
  test("ArrowArray.make Long array with cast") {
    val values: Seq[Int] = Seq(1, 2, 3, 4, 5)
    val array = ArrowArray(values:_*)

    // Assert is instance IntegralArray
    assert(array.isInstanceOf[IntArray])
    assert(array.as[Long].isInstanceOf[LogicalArray[_, Int, Long]])
    assertEquals(array.as[Long].toSeq, values.map(_.toLong))
  }

  test("ArrowArray.make UInt array with cast") {
    val values = Seq(1, 2, 3, 4, 5).map(UInt.trunc)
    val array = ArrowArray(values:_*).toSeq

    assertEquals(array, values)
  }

  test("ArrowArray.as should cast to optional" ) {
    val values = Seq(1, 2, 3, 4, 5)
    val array = ArrowArray(values:_*).as[Option[Int]]

    assertEquals(array.toSeq, values.map(Option.apply))
  }

  test("ArrowArray.as should cast to optional other type") {
    val values: Seq[Int] = Seq(1, 2, 3, 4, 5)
    val array = ArrowArray(values:_*)

    assertEquals(
      array.as[Option[Long]].toSeq, values.map(v => Option(v.toLong))
    )
    assertEquals(
      array.as[Option[Double]].toSeq, values.map(v => Option(v.toDouble))
    )
  }
}

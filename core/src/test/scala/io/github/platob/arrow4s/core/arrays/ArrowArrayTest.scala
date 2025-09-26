package io.github.platob.arrow4s.core.arrays

import io.github.platob.arrow4s.core.values.UInt
import munit.FunSuite

class ArrowArrayTest extends FunSuite {
  test("ArrowArray.make Long array with cast") {
    val values = Seq(1, 2, 3, 4, 5)
    val array = ArrowArray(values:_*).toSeq

    assertEquals(array, values)
  }

  test("ArrowArray.make UInt array with cast") {
    val values = Seq(1, 2, 3, 4, 5).map(UInt.trunc)
    val array = ArrowArray(values:_*).toSeq

    assertEquals(array, values)
  }

  test("ArrowArray.as should cast to optional" ) {
    val values = Seq(1, 2, 3, 4, 5)
    val array = ArrowArray(values:_*).as[Option[Int]].toSeq

    assertEquals(array, values.map(Option.apply))
  }
}

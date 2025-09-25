package io.github.platob.arrow4s.core.arrays

import io.github.platob.arrow4s.core.cast.NumericOpsPlus._
import io.github.platob.arrow4s.core.values.UInt
import munit.FunSuite

class ArrowArrayTest extends FunSuite {
  test("ArrowArray.make Long array with cast") {
    val values: Seq[Int] = Seq(1, 2, 3, 4, 5)
    val array = ArrowArray.make[Long](values.map(_.toLong):_*).toSeq

    assertEquals(array, values.map(_.toLong))
  }

  test("ArrowArray.make UInt array with cast") {
    val values: Seq[Int] = Seq(1, 2, 3, 4, 5)
    val array = ArrowArray.make[UInt](values.map(UInt.trunc):_*).toSeq

    assertEquals(array, values.map(UInt.trunc))
  }

  test("ArrowArray.make INT with optional") {
    val values: Seq[Option[Int]] = Seq(Some(1), None, Some(3), Some(4), None)
    val array = ArrowArray.makeOption(values: _*).toSeqOption

    assertEquals(array, values)
  }
}

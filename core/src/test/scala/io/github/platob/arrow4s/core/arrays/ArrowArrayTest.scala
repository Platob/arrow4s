package io.github.platob.arrow4s.core.arrays

import io.github.platob.arrow4s.core.arrays.ArrowArrayTest.TestRecord
import io.github.platob.arrow4s.core.arrays.primitive.IntegralArray.IntArray
import io.github.platob.arrow4s.core.values.UInt
import munit.FunSuite
import org.apache.arrow.vector.IntVector

class ArrowArrayTest extends FunSuite {
  val values: Seq[Int] = 0 until 10000

  test("ArrowArray.make Long array with cast") {
    val array = ArrowArray(values:_*)

    // Assert is instance IntegralArray
    assert(array.isInstanceOf[IntArray])
    assert(array.as[Int].isInstanceOf[IntArray])
    assert(array.as[Long].isInstanceOf[LogicalArray[IntVector, Int, Long]])
    assertEquals(array.as[Long], values.map(_.toLong))
  }

  test("ArrowArray.make String array") {
    val stringValues: Seq[String] = Seq("a", "b", "c", "d", "e")
    val array = ArrowArray(stringValues:_*)

    assertEquals(array, stringValues)
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

  test("ArrowArray.as should cast to non-optional") {
    val values: Seq[Option[Int]] = Seq(Some(1), Some(2), None, Some(4), Some(5))
    val array = ArrowArray(values:_*)

    assert(array.as[Int].isInstanceOf[IntArray])
    assertEquals(array.as[Int], values.map(_.getOrElse(null)))
    assertEquals(array.as[Long], values.map(v => v.map(_.toLong).getOrElse(null)))
    assertEquals(array.as[Double], values.map(v => v.map(_.toDouble).getOrElse(null)))
  }

  test("ArrowArray.make case class array") {
    val records = (1 to 5).map(i => TestRecord(i, s"str_$i", if (i % 2 == 0) Some(i.toDouble) else None))
    val array = ArrowArray(records:_*)
    val tuples = array.as[(Int, String, Option[Double])]

    assertEquals(array.child("a").as[Int], records.map(_.a))
    assertEquals(array.child("b").as[String], records.map(_.b))
    assertEquals(array.child("c").as[Option[Double]], records.map(_.c))
    assertEquals(array.get(0), records.head)
    assertEquals(array, records)
    assertEquals(tuples, records.map(r => (r.a, r.b, r.c)))
  }
}

object ArrowArrayTest {
  case class TestRecord(a: Int, b: String, c: Option[Double])
}

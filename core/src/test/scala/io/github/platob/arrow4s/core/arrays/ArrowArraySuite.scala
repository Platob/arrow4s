package io.github.platob.arrow4s.core.arrays

import io.github.platob.arrow4s.core.arrays.ArrowArraySuite.TestRecord
import io.github.platob.arrow4s.core.arrays.primitive.IntegralArray.IntArray
import io.github.platob.arrow4s.core.values.UInt
import munit.FunSuite
import org.apache.arrow.vector.IntVector

class ArrowArraySuite extends FunSuite {
  val values: Seq[Int] = 0 until 5

  test("ArrowArray.make Long array with cast") {
    val array = ArrowArray(values:_*)

    // Assert is instance IntegralArray
    assert(array.isInstanceOf[IntArray])
    assert(array.as[Int].isInstanceOf[IntArray])
    assert(array.as[Long].isInstanceOf[LogicalArray[IntArray, IntVector, Int, Long]])
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
    assertEquals(array.nullCount, 1)
    assertEquals(array.as[Int], values.map(_.getOrElse(null)))
    assertEquals(array.as[Long], values.map(v => v.map(_.toLong).getOrElse(null)))
    assertEquals(array.as[Double], values.map(v => v.map(_.toDouble).getOrElse(null)))
  }

  test("ArrowArray.slice should return sub-array") {
    val array = ArrowArray(values:_*)

    val slice = array.slice(1, 4)

    assertEquals(slice.length, 3)
    assertEquals(slice.as[Int], values.slice(1, 4))
  }

  test("ArrowArray.make list array") {
    val listValues: Seq[Seq[Int]] = Seq(
      Seq(1, 2, 3),
      Seq(4, 5),
      Seq(),
      Seq(6, 7, 8, 9)
    )
    val array = ArrowArray(listValues:_*)

    assertEquals(array.as[Seq[Int]], listValues)
  }

  test("ArrowArray.make case class array") {
    val records = (1 to 5).map(i => TestRecord(i, s"str_$i", if (i % 2 == 0) Some(i.toDouble) else None))
    val array = ArrowArray(records:_*)
    val tuples = array.as[(Int, String, Option[Double])]
    val first = array.get(0)

    assertEquals(array.child("a").as[Int], records.map(_.a))
    assertEquals(array.child("b").as[String], records.map(_.b))
    assertEquals(array.child("c").as[Option[Double]], records.map(_.c))
    assertEquals(first, records.head)
    assertEquals(array, records)
    assertEquals(tuples, records.map(r => (r.a, r.b, r.c)))
  }

  test("ArrowArray.make map array") {
    val mapValues: Seq[Map[String, Int]] = Seq(
      Map("a" -> 1, "b" -> 2),
      Map("c" -> 3),
      Map(),
      Map("d" -> 4, "e" -> 5, "f" -> 6)
    )
    val array = ArrowArray(mapValues:_*)

    assertEquals(array.as[Map[String, Int]], mapValues)

    // Test array map mutations
//    array.set(2, Map("x" -> 10, "y" -> 20))
    array.append(Map("z" -> 30))

    val newValues = mapValues :+ Map("z" -> 30)
    assertEquals(array.as[Map[String, Int]], newValues)
  }

  test("ArrowArray mutable operations") {
    val array = ArrowArray(values.map(Option(_)):_*)

    // Test set
    array.set(2, None)
    array.set(values.length, Some(100))

    assertEquals(array.nullCount, 1)
    assertEquals(array.get(values.length), Some(100))
    // Unchanged length
    assertEquals(array.length, values.length)

    // Test append
    array.append(None).append(Some(101))

    assertEquals(array.nullCount, 2)
    assertEquals(array.length, values.length + 2)
    assertEquals(array.get(values.length), None)
    assertEquals(array.get(values.length + 1), Some(101))
  }
}

object ArrowArraySuite {
  case class TestRecord(a: Int, b: String, c: Option[Double])
}

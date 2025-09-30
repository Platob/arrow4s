// src/test/scala/io/github/platob/arrow4s/core/cast/TypeConverterSuite.scala
package io.github.platob.arrow4s.core.cast

import io.github.platob.arrow4s.core.values._
import munit.FunSuite

import scala.util.Try

final class TypeConverterSuite extends FunSuite {

  // Keep global cache pollution predictable: only register what we need for tests.
  override def beforeAll(): Unit = {
    // primitive lanes
    TypeConverter.register(TypeConverter.intToLong)
    TypeConverter.register(TypeConverter.intToBytes)
    // booleans/strings used directly (no cache needed), but fine to register too
    TypeConverter.register(TypeConverter.booleanToString)
  }

  test("identity preserves value and round-trips") {
    val id = TypeConverter.identity[Int]
    assertEquals(id.to(42), 42)
    assertEquals(id.from(42), 42)
  }

  test("instance builds a reversible converter") {
    val times2 = TypeConverter.instance[Int, Int](_ * 2, _ / 2)
    assertEquals(times2.to(9), 18)
    assertEquals(times2.reverse.to(5), 2) // reverse: /2
  }

  test("register + get: Int -> Long works") {
    val c = TypeConverter.get[Int, Long]
    assertEquals(c.to(7), 7L)
    assertEquals(c.from(7L), 7)
  }

  test("register also exposes reverse: Long -> Int") {
    val c = TypeConverter.get[Long, Int]
    assertEquals(c.to(123L), 123)
    assertEquals(c.from(456), 456L)
  }

  test("optional: Option[Int] <-> Option[Long]") {
    val c = TypeConverter.get[Option[Int], Option[Long]]
    assertEquals(c.to(Some(5)), Some(5L))
    assertEquals(c.to(None), None)
    assertEquals(c.from(Some(9L)), Some(9))
    assertEquals(c.from(None), None)
  }

  test("optionalSource: Option[Int] -> Long (None blows up)") {
    val c = TypeConverter.get[Option[Int], Long]
    assertEquals(c.to(Some(2)), 2L)
    assert(Try(c.to(None)).failed.get.isInstanceOf[NoSuchElementException])
    // from goes Long -> Option[Int]
    assertEquals(c.from(10L), Some(10))
  }

  test("optionalTarget: Int -> Option[Long] (from None blows up)") {
    val c = TypeConverter.get[Int, Option[Long]]
    assertEquals(c.to(3), Some(3L))
    // from: Option[Long] -> Int
    assertEquals(c.from(Some(8L)), 8)
    assert(Try(c.from(None)).failed.get.isInstanceOf[NoSuchElementException])
  }

  test("bytes round-trip: Int <-> Array[Byte]") {
    val toBytes   = TypeConverter.get[Int, Array[Byte]]
    val fromBytes = TypeConverter.get[Array[Byte], Int]

    val n = 0x12345678
    val arr = toBytes.to(n)
    assertEquals(arr.length, 4)
    assertEquals(fromBytes.to(arr), n)
  }

  test("boolean <-> string direct converters (no cache)") {
    val b2s = TypeConverter.booleanToString
    val s2b = TypeConverter.stringToBoolean

    assertEquals(b2s.to(true), "true")
    assertEquals(b2s.to(false), "false")

    assertEquals(s2b.to("true"), true)
    assertEquals(s2b.to("false"), false)
    assertEquals(s2b.to("Yes"), true)  // 'Y'
    assertEquals(s2b.to("0"), false)   // '0'
    intercept[IllegalArgumentException] {
      s2b.to("?") // not in the accepted head-chars
    }
  }

  test("no converter found -> throws") {
    import scala.reflect.runtime.{universe => ru}
    intercept[NoSuchElementException] {
      TypeConverter.get(ru.typeOf[Char], ru.typeOf[Int]) // nothing registered for Char -> Int
    }
  }

  test("unsigned helpers: UByte <-> Byte direct vals behave") {
    val up = TypeConverter.byteToUByte
    val down = TypeConverter.ubyteToByte
    assertEquals(up.to(-1.toByte).toInt & 0xff, 255)
    assertEquals(down.to(UByte(255)), -1.toByte)
  }
}

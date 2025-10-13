package io.github.platob.arrow4s.core.values

import io.github.platob.arrow4s.core.codec.value.primitive.PrimitiveValueCodec
import munit.FunSuite

import scala.reflect.runtime.{universe => ru}

final class Int32CodecSuite extends FunSuite {
  private val i32: PrimitiveValueCodec[Int] = implicitly[PrimitiveValueCodec[Int]]

  test("summon ValueCodec[Int] from companion") {
    assertEquals(i32.namespace, "scala.Int")
  }

  test("metadata is sane") {
    assertEquals(i32.bitSize, 32)
    assertEquals(i32.default, 0)
    assertEquals(i32.children, Seq.empty)
    assertEquals(i32.tpe, ru.typeOf[Int])
  }

  test("toBytes/fromBytes round-trip for a few ints") {
    val samples = List(0, 1, -1, 42, Int.MinValue, Int.MaxValue, 0x78563412)
    samples.foreach { n =>
      val bytes = i32.toBytes(n)
      val back  = i32.fromBytes(bytes)
      assertEquals(back, n, clues(bytes.mkString("[", ",", "]")))
    }
  }

  test("byte layout is little-endian as implemented") {
    // 0x78563412 -> bytes [0x12, 0x34, 0x56, 0x78]
    val n = 0x78563412
    val b = i32.toBytes(n).map(_ & 0xFF).toList
    assertEquals(b, List(0x12, 0x34, 0x56, 0x78))
    // sanity: reconstructs the same int
    assertEquals(i32.fromBytes(i32.toBytes(n)), n)
  }

  test("negative numbers serialize and deserialize") {
    val n  = -1
    val bs = i32.toBytes(n)
    // all 0xFF in two's complement little-endian
    assert(bs.forall(_ == 0xFF.toByte))
    assertEquals(i32.fromBytes(bs), -1)
  }

  test("string conversions") {
    assertEquals(i32.toString(1337), "1337")
    assertEquals(i32.fromString("1337"), 1337)
  }

  test("numeric conversions") {
    val n = 123456789
    assertEquals(i32.toInt(n), n)
    assertEquals(i32.toLong(n), n.toLong)
    assertEquals(i32.toDouble(n), n.toDouble)
    assertEquals(i32.fromInt(n), n)
    assertEquals(i32.fromLong(n.toLong), n)
    // fromDouble truncates per implementation
    assertEquals(i32.fromDouble(1.9), 1)
  }

  test("big number bridges") {
    val n = 123
    val bi = i32.toBigInteger(n)
    val bd = i32.toBigDecimal(n)
    assertEquals(bi, java.math.BigInteger.valueOf(123L))
    assertEquals(bd, java.math.BigDecimal.valueOf(123L))
    assertEquals(i32.fromBigInteger(bi), 123)
    assertEquals(i32.fromBigDecimal(bd), 123)
  }
}


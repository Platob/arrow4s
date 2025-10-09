package io.github.platob.arrow4s.core.values

import munit.FunSuite

final class UByteSuite extends FunSuite {

  // tiny helper to build from Ints 0..255
  private def ub(i: Int): UByte = UByte.trunc(i)

  test("constants: Min/Max/Zero/One") {
    assertEquals(UByte.MinValue.toInt, 0)
    assertEquals(UByte.Zero.toInt, 0)
    assertEquals(UByte.One.toInt, 1)
    assertEquals(UByte.MaxValue.toInt, 255)
  }

  test("toString is decimal of toInt") {
    assertEquals(ub(0).toString, "0")
    assertEquals(ub(1).toString, "1")
    assertEquals(ub(255).toString, "255")
    assertEquals(ub(200).toString, "200")
  }

  test("boolean/byte/short/int/long/float/double conversions are consistent") {
    (0 to 255).foreach { i =>
      val u = ub(i)
      assertEquals(u.toBoolean, i != 0, clues(i))
      // byte is signed on JVM; confirm two's complement mapping
      assertEquals(u.toByte, (i & 0xFF).toByte, clues(i))
      assertEquals(u.toShort, (i & 0xFF).toShort, clues(i))
      assertEquals(u.toInt, i & 0xFF, clues(i))
      assertEquals(u.toLong, (i & 0xFF).toLong, clues(i))
      assertEquals(u.toFloat, (i & 0xFF).toFloat, clues(i))
      assertEquals(u.toDouble, (i & 0xFF).toDouble, clues(i))
    }
  }

  test("trunc from various widths masks to 0xFF") {
    // Int
    assertEquals(UByte.trunc(256).toInt, 0)
    assertEquals(UByte.trunc(-1).toInt, 255)
    assertEquals(UByte.trunc(511).toInt, 255)
    // Long
    assertEquals(UByte.trunc(256L).toInt, 0)
    assertEquals(UByte.trunc(-1L).toInt, 255)
    // Short
    assertEquals(UByte.trunc(256.toShort).toInt, 0)
  }

  test("unsafe keeps raw byte; observable after toInt masking") {
    val uNeg1 = UByte.unsafe(-1.toByte)
    val u127  = UByte.unsafe(127.toByte)
    assertEquals(uNeg1.toInt, 255) // -1 byte -> 0xFF -> 255
    assertEquals(u127.toInt, 127)
  }

  test("addition wraps modulo 256") {
    val pairs = List(
      (0, 0, 0),
      (1, 2, 3),
      (250, 10, 4),   // 260 -> 4
      (255, 1, 0),
      (200, 200, 144) // 400 -> 144
    )
    pairs.foreach { case (a, b, sum) =>
      assertEquals((ub(a) + ub(b)).toInt, sum, clues(a, b))
    }
    // quick sweep
    (0 to 255).foreach { a =>
      (0 to 255 by 17).foreach { b =>
        val exp = (a + b) & 0xFF
        assertEquals((ub(a) + ub(b)).toInt, exp, clues(a, b))
      }
    }
  }

  test("subtraction wraps modulo 256") {
    val pairs = List(
      (0, 0, 0),
      (2, 1, 1),
      (0, 1, 255),
      (5, 250, 11) // 5-250 = -245 -> 11
    )
    pairs.foreach { case (a, b, diff) =>
      assertEquals((ub(a) - ub(b)).toInt, diff, clues(a, b))
    }
  }

  test("multiplication wraps modulo 256") {
    val pairs = List(
      (0, 0, 0),
      (3, 5, 15),
      (20, 20, 144), // 400 -> 144
      (16, 16, 0)    // 256 -> 0
    )
    pairs.foreach { case (a, b, prod) =>
      assertEquals((ub(a) * ub(b)).toInt, prod, clues(a, b))
    }
  }

  test("division uses integer division; divide by zero throws") {
    assertEquals((ub(10) / ub(2)).toInt, 5)
    assertEquals((ub(255) / ub(10)).toInt, 25) // 255 / 10 = 25
    intercept[ArithmeticException] {
      val _ = ub(5) / UByte.Zero
    }
  }

  test("modulo matches Int modulo") {
    val pairs = List(
      (10, 3, 1),
      (255, 10, 5),
      (0, 7, 0)
    )
    pairs.foreach { case (a, b, r) =>
      assertEquals((ub(a) % ub(b)).toInt, r, clues(a, b))
    }
    intercept[ArithmeticException] {
      val _ = ub(5) % UByte.Zero
    }
  }

  test("unary minus wraps (two's complement)") {
    assertEquals((-ub(0)).toInt, 0)
    assertEquals((-ub(1)).toInt, 255)
    assertEquals((-ub(5)).toInt, 251)
    assertEquals((-ub(128)).toInt, 128) // 256-128
  }

  test("compare uses unsigned numeric order") {
    // simple checks
    assert(ub(10).compare(ub(20)) < 0)
    assert(ub(200).compare(ub(199)) > 0)
    assertEquals(ub(0).compare(ub(0)), 0)

    // total order over 0..255
    val xs = (0 to 255).map(ub)
    val sorted = xs.sortWith((a, b) => a.compare(b) < 0)
    assertEquals(sorted.map(_.toInt), 0 to 255)
  }
}

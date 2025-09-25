
package io.github.platob.arrow4s.core.values

import io.github.platob.arrow4s.core.cast.{AnyOpsPlus, NumericOpsPlus}
import munit.FunSuite

class UUnsignedSuite extends FunSuite {

  test("UByte basic conversions and wrap") {
    val a = UByte(255)
    assertEquals(a.toInt, 255)
    val b = UByte.trunc(256 + 3)
    assertEquals(b.toInt, 3)

    // Numeric ops via AnyOpsPlus
    val N = implicitly[NumericOpsPlus[UByte]]
    assertEquals(N.plus(UByte(250), UByte(10)).toInt, 4) // wrap
    assert(N.compare(UByte(1), UByte(2)) < 0)
  }

  test("UShort conversions") {
    val u = UShort(65535)
    assertEquals(u.toInt, 65535)
    val v = UShort.trunc(65536 + 2)
    assertEquals(v.toInt, 2)
  }
}

package io.github.platob.arrow4s.core.values

import munit.FunSuite

class UByteTest extends FunSuite {
  test("UByte.trunc should truncate values correctly") {
    assertEquals(UByte.trunc(0), UByte(0))
    assertEquals(UByte.trunc(255), UByte(255))
    assertEquals(UByte.trunc(256), UByte(0))
    assertEquals(UByte.trunc(-1), UByte(255))
    assertEquals(UByte.trunc(511), UByte(255))
  }
}

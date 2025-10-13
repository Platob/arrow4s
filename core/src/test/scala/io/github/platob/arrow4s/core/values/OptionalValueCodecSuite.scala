package io.github.platob.arrow4s.core.values

import io.github.platob.arrow4s.core.codec.value.ValueCodec
import munit.FunSuite

final class OptionalValueCodecSuite extends FunSuite {

  import ValueCodec._

  // Optional[Int] using the built-in Int32 codec as inner
  private val C = implicitly[ValueCodec[Option[Int]]]

  test("metadata and structure") {
    assertEquals(C.namespace, "scala.Option")
    assertEquals(C.bitSize, -1)                 // VariableLength
    assertEquals(C.children.size, 1)
    assertEquals(C.children.head.namespace, "scala.Int")
    assertEquals(C.default, None)
    assertEquals(C.arity, 1)
  }

  test("toArray / fromArray: None uses empty array; Some delegates to inner") {
    // None -> empty
    assert(C.elements(None).isEmpty)
    assertEquals(C.fromElements(Array.empty[Any]), None)

    // Some -> inner’s array
    val innerArr = int.elements(42)
    assertEquals(C.elements(Some(42)).toList, innerArr.toList)
    assertEquals(C.fromElements(innerArr), Some(42))
  }

  test("toBytes / fromBytes: empty for None; delegates for Some") {
    val bytesSome = C.toBytes(Some(1337))
    assertEquals(bytesSome.toList, int.toBytes(1337).toList)

    val bytesNone = C.toBytes(None)
    assertEquals(bytesNone.isEmpty, true)

    assertEquals(C.fromBytes(bytesNone), None)
    assertEquals(C.fromBytes(bytesSome), Some(1337))
  }

  test("toString / fromString: '' and 'null' map to None; otherwise delegate") {
    assertEquals(C.toString(None), "")
    assertEquals(C.fromString(""), None)
    assertEquals(C.fromString("null"), None)
    assertEquals(C.fromString("NuLl"), None)

    assertEquals(C.toString(Some(7)), "7")
    assertEquals(C.fromString("7"), Some(7))
  }

  test("numeric bridges: None maps to zeros; from* wraps inner results in Some") {
    assertEquals(C.toInt(None), 0)
    assertEquals(C.toLong(None), 0L)
    assertEquals(C.toDouble(None), 0.0)
    assertEquals(C.toBigInteger(None), java.math.BigInteger.ZERO)
    assertEquals(C.toBigDecimal(None), java.math.BigDecimal.ZERO)

    assertEquals(C.fromInt(5), Some(5))
    assertEquals(C.fromLong(9L), Some(9))
    assertEquals(C.fromDouble(2.9), Some(2)) // inner truncates
    assertEquals(C.fromBigInteger(java.math.BigInteger.valueOf(123L)), Some(123))
    assertEquals(C.fromBigDecimal(java.math.BigDecimal.valueOf(456L)), Some(456))
  }

  test("round-trip sanity: bytes, array, string") {
    val values = List(None, Some(0), Some(1), Some(-1), Some(Int.MaxValue), Some(Int.MinValue))
    values.foreach { v =>
      val b = C.fromBytes(C.toBytes(v))
      val a = C.fromElements(C.elements(v))
      val s = C.fromString(C.toString(v))
      assertEquals(b, v, clues(v))
      assertEquals(a, v, clues(v))
      assertEquals(s, v, clues(v))
    }
  }
}

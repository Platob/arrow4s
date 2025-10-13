package io.github.platob.arrow4s.core.codec.value.primitive

import io.github.platob.arrow4s.core.codec.value.ValueCodec
import io.github.platob.arrow4s.core.types.ArrowField
import io.github.platob.arrow4s.core.values.UByte
import org.apache.arrow.vector.types.pojo.ArrowType

import java.math.BigInteger
import java.nio.charset.Charset
import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}

abstract class ByteBasedValueCodec[T : ru.TypeTag : ClassTag] extends PrimitiveValueCodec[T] {

}

object ByteBasedValueCodec {
  val UTF8_CHARSET: Charset = Charset.forName("UTF-8")

  class BinaryValueCodec extends ByteBasedValueCodec[Array[Byte]] {
    override def namespace: String = "Binary"
    override def bitSize: Int = -1 // Variable length
    override val zero: Array[Byte] = Array.emptyByteArray
    override val one: Array[Byte] = Array(1.toByte)

    override def arrowField: org.apache.arrow.vector.types.pojo.Field =
      ArrowField.build(
        namespace,
        at = ArrowType.Binary.INSTANCE,
        nullable = false,
        children = Seq.empty,
        metadata = None
      )

    override def toBytes(value: Array[Byte]): Array[Byte] = value
    override def fromBytes(bytes: Array[Byte]): Array[Byte] = bytes

    override def toString(value: Array[Byte], charset: Charset): String = new String(value, charset)
    override def fromString(string: String, charset: Charset): Array[Byte] = string.getBytes(charset)

    override def toBoolean(value: Array[Byte]): Boolean = ValueCodec.boolean.fromBytes(value)
    override def fromBoolean(value: Boolean): Array[Byte] = ValueCodec.boolean.toBytes(value)

    override def toByte(value: Array[Byte]): Byte = ValueCodec.byte.fromBytes(value)
    override def fromByte(value: Byte): Array[Byte] = ValueCodec.byte.toBytes(value)

    override def toUByte(value: Array[Byte]): UByte = ValueCodec.ubyte.fromBytes(value)
    override def fromUByte(value: UByte): Array[Byte] = ValueCodec.ubyte.toBytes(value)

    override def toShort(value: Array[Byte]): Short = ValueCodec.short.fromBytes(value)
    override def fromShort(value: Short): Array[Byte] = ValueCodec.short.toBytes(value)

    override def toChar(value: Array[Byte]): Char = ValueCodec.char.fromBytes(value)
    override def fromChar(value: Char): Array[Byte] = ValueCodec.char.toBytes(value)

    override def toInt(value: Array[Byte]): Int = ValueCodec.int.fromBytes(value)
    override def fromInt(value: Int): Array[Byte] = ValueCodec.int.toBytes(value)

    override def toLong(value: Array[Byte]): Long = ValueCodec.long.fromBytes(value)
    override def fromLong(value: Long): Array[Byte] = ValueCodec.long.toBytes(value)

    override def toFloat(value: Array[Byte]): Float = ValueCodec.float.fromBytes(value)
    override def fromFloat(value: Float): Array[Byte] = ValueCodec.float.toBytes(value)

    override def toDouble(value: Array[Byte]): Double = ValueCodec.double.fromBytes(value)
    override def fromDouble(value: Double): Array[Byte] = ValueCodec.double.toBytes(value)

    override def toBigInteger(value: Array[Byte]): BigInteger = ValueCodec.bigInteger.fromBytes(value)
    override def fromBigInteger(value: BigInteger): Array[Byte] = ValueCodec.bigInteger.toBytes(value)

    override def toBigDecimal(value: Array[Byte]): java.math.BigDecimal = ValueCodec.bigDecimal.fromBytes(value)
    override def fromBigDecimal(value: java.math.BigDecimal): Array[Byte] = ValueCodec.bigDecimal.toBytes(value)
  }

  class StringValueCodec extends ByteBasedValueCodec[String] {
    override def namespace: String = "String"

    override def bitSize: Int = -1 // Variable length

    override val zero: String = ""
    override val one: String = "1"

    override def arrowField: org.apache.arrow.vector.types.pojo.Field =
      ArrowField.build(
        namespace,
        at = ArrowType.Utf8.INSTANCE,
        nullable = false,
        children = Seq.empty,
        metadata = None
      )

    override def toBytes(value: String): Array[Byte] = value.getBytes(UTF8_CHARSET)
    override def fromBytes(bytes: Array[Byte]): String = new String(bytes, UTF8_CHARSET)

    override def toString(value: String, charset: Charset): String = value
    override def fromString(string: String, charset: Charset): String = string

    override def toBoolean(value: String): Boolean = ValueCodec.boolean.fromString(value)
    override def fromBoolean(value: Boolean): String = ValueCodec.boolean.toString(value)

    override def toByte(value: String): Byte = ValueCodec.byte.fromString(value)
    override def fromByte(value: Byte): String = ValueCodec.byte.toString(value)

    override def toUByte(value: String): UByte = ValueCodec.ubyte.fromString(value)
    override def fromUByte(value: UByte): String = ValueCodec.ubyte.toString(value)

    override def toShort(value: String): Short = ValueCodec.short.fromString(value)
    override def fromShort(value: Short): String = ValueCodec.short.toString(value)

    override def toChar(value: String): Char = ValueCodec.char.fromString(value)
    override def fromChar(value: Char): String = ValueCodec.char.toString(value)

    override def toInt(value: String): Int = ValueCodec.int.fromString(value)
    override def fromInt(value: Int): String = ValueCodec.int.toString(value)

    override def toLong(value: String): Long = ValueCodec.long.fromString(value)
    override def fromLong(value: Long): String = ValueCodec.long.toString(value)

    override def toFloat(value: String): Float = ValueCodec.float.fromString(value)
    override def fromFloat(value: Float): String = ValueCodec.float.toString(value)

    override def toDouble(value: String): Double = ValueCodec.double.fromString(value)
    override def fromDouble(value: Double): String = ValueCodec.double.toString(value)

    override def toBigInteger(value: String): BigInteger = ValueCodec.bigInteger.fromString(value)
    override def fromBigInteger(value: BigInteger): String = ValueCodec.bigInteger.toString(value)

    override def toBigDecimal(value: String): java.math.BigDecimal = ValueCodec.bigDecimal.fromString(value)
    override def fromBigDecimal(value: java.math.BigDecimal): String = ValueCodec.bigDecimal.toString(value)
  }
}
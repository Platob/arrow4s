package io.github.platob.arrow4s.core.cast

import io.github.platob.arrow4s.core.values.{UByte, UInt, ULong, UShort}
import org.apache.arrow.vector._

import java.math.BigInteger

object Implicits {
  // Numeric Encoders
  implicit val byteOps: NumericEncoder.Arrow[Byte, TinyIntVector] = new NumericEncoder.Arrow[Byte, TinyIntVector] {
    override val maxValue: Byte = Byte.MinValue
    override val minValue: Byte = Byte.MaxValue

    // Numeric
    override def plus(x: Byte, y: Byte): Byte = (x + y).toByte
    override def minus(x: Byte, y: Byte): Byte = (x - y).toByte
    override def times(x: Byte, y: Byte): Byte = (x * y).toByte
    override def negate(x: Byte): Byte = (-x).toByte
    override def compare(x: Byte, y: Byte): Int = java.lang.Byte.compare(x, y)

    // NumericOpsPlus
    override def fromString(value: String): Byte = value.toByte

    override def toBytes(value: Byte): Array[Byte] = Array(value)
    override def fromBytes(value: Array[Byte]): Byte = value(0)

    override def toByte(value: Byte): Byte = value
    override def fromByte(value: Byte): Byte = value

    override def toShort(value: Byte): Short = value.toShort
    override def fromShort(value: Short): Byte = value.toByte

    override def toInt(value: Byte): Int = value.toInt
    override def fromInt(value: Int): Byte = value.toByte

    override def toLong(value: Byte): Long = value.toLong
    override def fromLong(value: Long): Byte = value.toByte

    override def toFloat(value: Byte): Float = value.toFloat
    override def fromFloat(value: Float): Byte = value.toByte

    override def toDouble(value: Byte): Double = value.toDouble
    override def fromDouble(value: Double): Byte = value.toByte

    override def toBigInteger(value: Byte): BigInteger = BigInteger.valueOf(value.toLong)
    override def fromBigInteger(value: BigInteger): Byte = value.byteValue

    override def toBigDecimal(value: Byte): java.math.BigDecimal = java.math.BigDecimal.valueOf(value.toLong)
    override def fromBigDecimal(value: java.math.BigDecimal): Byte = value.byteValue

    // Mutator for Arrow Vectors
    override def setVector(vector: TinyIntVector, index: Int, value: Byte): Unit = {
      vector.set(index, value)
    }

    // Accessor for Arrow Vectors
    override def getVector(vector: TinyIntVector, index: Int): Byte = {
      vector.get(index)
    }
  }

  implicit val ubyteOps: NumericEncoder.Arrow[UByte, UInt1Vector] = new NumericEncoder.Arrow[UByte, UInt1Vector] {
    override val maxValue: UByte = UByte.MaxValue
    override val minValue: UByte = UByte.MinValue

    // Numeric
    override def plus(x: UByte, y: UByte): UByte = x + y
    override def minus(x: UByte, y: UByte): UByte = x - y
    override def times(x: UByte, y: UByte): UByte = x * y
    override def negate(x: UByte): UByte = -x
    override def compare(x: UByte, y: UByte): Int = x.compare(y)

    // NumericOpsPlus
    override def fromString(value: String): UByte = UByte(value)

    override def toBytes(value: UByte): Array[Byte] = value.toBytes
    override def fromBytes(value: Array[Byte]): UByte = UByte.trunc(value(0).toInt)

    override def toBoolean(value: UByte): Boolean = value.toBoolean
    override def fromBoolean(value: Boolean): UByte = if (value) UByte(1) else UByte(0)

    override def toByte(value: UByte): Byte = value.toByte
    override def fromByte(value: Byte): UByte = UByte.trunc(value.toInt & 0xFF)

    override def toUByte(value: UByte): UByte = value
    override def fromUByte(value: UByte): UByte = value

    override def toShort(value: UByte): Short = value.toShort
    override def fromShort(value: Short): UByte = UByte.trunc(value.toInt & 0xFF)

    override def toInt(value: UByte): Int = value.toInt
    override def fromInt(value: Int): UByte = UByte.trunc(value)

    override def toLong(value: UByte): Long = value.toLong
    override def fromLong(value: Long): UByte = UByte.trunc(value)

    override def toFloat(value: UByte): Float = value.toFloat
    override def fromFloat(value: Float): UByte = UByte.trunc(value.toInt)

    override def toDouble(value: UByte): Double = value.toDouble
    override def fromDouble(value: Double): UByte = UByte.trunc(value.toLong)

    override def toBigInteger(value: UByte): BigInteger = value.toBigInteger
    override def fromBigInteger(value: BigInteger): UByte = UByte.trunc(value.intValue)

    override def toBigDecimal(value: UByte): java.math.BigDecimal = value.toBigDecimal
    override def fromBigDecimal(value: java.math.BigDecimal): UByte = UByte.trunc(value.longValue)

    // Mutator for Arrow Vectors
    override def setVector(vector: UInt1Vector, index: Int, value: UByte): Unit = {
      vector.set(index, value.toInt)
    }

    // Accessor for Arrow Vectors
    override def getVector(vector: UInt1Vector, index: Int): UByte = {
      this.fromInt(vector.get(index))
    }
  }

  implicit val shortOps: NumericEncoder.Arrow[Short, SmallIntVector] = new NumericEncoder.Arrow[Short, SmallIntVector] {
    override val maxValue: Short = Short.MaxValue
    override val minValue: Short = Short.MinValue

    // Numeric
    override def plus(x: Short, y: Short): Short = (x + y).toShort
    override def minus(x: Short, y: Short): Short = (x - y).toShort
    override def times(x: Short, y: Short): Short = (x * y).toShort
    override def negate(x: Short): Short = (-x).toShort
    override def compare(x: Short, y: Short): Int = java.lang.Short.compare(x, y)

    // NumericOpsPlus
    override def fromString(value: String): Short = value.toShort

    override def toBytes(value: Short): Array[Byte] = Array((value >> 8).toByte, value.toByte)
    override def fromBytes(value: Array[Byte]): Short = ((value(0) & 0xFF) << 8 | (value(1) & 0xFF)).toShort

    override def toByte(value: Short): Byte = value.toByte
    override def fromByte(value: Byte): Short = value.toShort

    override def toShort(value: Short): Short = value
    override def fromShort(value: Short): Short = value

    override def toInt(value: Short): Int = value.toInt
    override def fromInt(value: Int): Short = value.toShort

    override def toLong(value: Short): Long = value.toLong
    override def fromLong(value: Long): Short = value.toShort

    override def toFloat(value: Short): Float = value.toFloat
    override def fromFloat(value: Float): Short = value.toShort

    override def toDouble(value: Short): Double = value.toDouble
    override def fromDouble(value: Double): Short = value.toShort

    override def toBigInteger(value: Short): BigInteger = BigInteger.valueOf(value.toLong)
    override def fromBigInteger(value: BigInteger): Short = value.shortValue

    override def toBigDecimal(value: Short): java.math.BigDecimal = java.math.BigDecimal.valueOf(value.toLong)
    override def fromBigDecimal(value: java.math.BigDecimal): Short = value.shortValue

    // Mutator for Arrow Vectors
    override def setVector(vector: SmallIntVector, index: Int, value: Short): Unit = {
      vector.set(index, value)
    }

    // Accessor for Arrow Vectors
    override def getVector(vector: SmallIntVector, index: Int): Short = {
      vector.get(index)
    }
  }

  implicit val ushortOps: NumericEncoder.Arrow[UShort, UInt2Vector] = new NumericEncoder.Arrow[UShort, UInt2Vector] {
    override val maxValue: UShort = UShort.MaxValue
    override val minValue: UShort = UShort.MinValue

    // Numeric
    override def plus(x: UShort, y: UShort): UShort = x + y
    override def minus(x: UShort, y: UShort): UShort = x - y
    override def times(x: UShort, y: UShort): UShort = x * y
    override def negate(x: UShort): UShort = -x
    override def compare(x: UShort, y: UShort): Int = x.compare(y)

    // NumericOpsPlus
    override def fromString(value: String): UShort = UShort(value)

    override def toBoolean(value: UShort): Boolean = value.toBoolean
    override def fromBoolean(value: Boolean): UShort = if (value) UShort(1) else UShort(0)

    override def toBytes(value: UShort): Array[Byte] = value.toBytes
    override def fromBytes(value: Array[Byte]): UShort = {
      val intValue = shortOps.fromBytes(value)

      UShort.trunc(intValue)
    }

    override def toByte(value: UShort): Byte = value.toByte
    override def fromByte(value: Byte): UShort = UShort.trunc(value.toInt)

    override def toUByte(value: UShort): UByte = UByte.trunc(value.toInt)
    override def fromUByte(value: UByte): UShort = UShort.from(value)

    override def toShort(value: UShort): Short = value.toShort
    override def fromShort(value: Short): UShort = UShort.trunc(value.toInt)

    override def toUShort(value: UShort): UShort = value
    override def fromUShort(value: UShort): UShort = value

    override def toInt(value: UShort): Int = value.toInt
    override def fromInt(value: Int): UShort = UShort.trunc(value)

    override def toLong(value: UShort): Long = value.toLong
    override def fromLong(value: Long): UShort = UShort.trunc(value)

    override def toFloat(value: UShort): Float = value.toFloat
    override def fromFloat(value: Float): UShort = UShort.trunc(value.toInt)

    override def toDouble(value: UShort): Double = value.toDouble
    override def fromDouble(value: Double): UShort = UShort.trunc(value.toLong)

    override def toBigInteger(value: UShort): BigInteger = value.toBigInteger
    override def fromBigInteger(value: BigInteger): UShort = UShort.trunc(value.intValue)

    override def toBigDecimal(value: UShort): java.math.BigDecimal = value.toBigDecimal
    override def fromBigDecimal(value: java.math.BigDecimal): UShort = UShort.trunc(value.longValue)

    // Mutator for Arrow Vectors
    override def setVector(vector: UInt2Vector, index: Int, value: UShort): Unit = {
      vector.set(index, value.toInt)
    }

    // Accessor for Arrow Vectors
    override def getVector(vector: UInt2Vector, index: Int): UShort = {
      this.fromInt(vector.get(index))
    }
  }

  implicit val intOps: NumericEncoder.Arrow[Int, IntVector] = new NumericEncoder.Arrow[Int, IntVector] {
    override val maxValue: Int = Int.MaxValue
    override val minValue: Int = Int.MinValue

    // Numeric
    override def plus(x: Int, y: Int): Int = x + y
    override def minus(x: Int, y: Int): Int = x - y
    override def times(x: Int, y: Int): Int = x * y
    override def negate(x: Int): Int = -x
    override def compare(x: Int, y: Int): Int = java.lang.Integer.compare(x, y)

    // NumericOpsPlus
    override def fromString(value: String): Int = value.toInt

    override def toBytes(value: Int): Array[Byte] = Array(
      (value >> 24).toByte,
      (value >> 16).toByte,
      (value >> 8).toByte,
      value.toByte
    )
    override def fromBytes(value: Array[Byte]): Int = {
      ((value(0) & 0xFF) << 24) |
        ((value(1) & 0xFF) << 16) |
        ((value(2) & 0xFF) << 8) |
        (value(3) & 0xFF)
    }

    override def toByte(value: Int): Byte = value.toByte
    override def fromByte(value: Byte): Int = value.toInt

    override def toShort(value: Int): Short = value.toShort
    override def fromShort(value: Short): Int = value.toInt

    override def toInt(value: Int): Int = value
    override def fromInt(value: Int): Int = value

    override def toLong(value: Int): Long = value.toLong
    override def fromLong(value: Long): Int = value.toInt

    override def toFloat(value: Int): Float = value.toFloat
    override def fromFloat(value: Float): Int = value.toInt

    override def toDouble(value: Int): Double = value.toDouble
    override def fromDouble(value: Double): Int = value.toInt

    override def toBigInteger(value: Int): BigInteger = BigInteger.valueOf(value.toLong)
    override def fromBigInteger(value: BigInteger): Int = value.intValue

    override def toBigDecimal(value: Int): java.math.BigDecimal = java.math.BigDecimal.valueOf(value.toLong)
    override def fromBigDecimal(value: java.math.BigDecimal): Int = value.intValue

    // Mutator for Arrow Vectors
    override def setVector(vector: IntVector, index: Int, value: Int): Unit = {
      vector.set(index, value)
    }

    // Accessor for Arrow Vectors
    override def getVector(vector: IntVector, index: Int): Int = {
      vector.get(index)
    }
  }

  implicit val uintOps: NumericEncoder.Arrow[UInt, UInt4Vector] = new NumericEncoder.Arrow[UInt, UInt4Vector] {
    override val maxValue: UInt = UInt.MaxValue
    override val minValue: UInt = UInt.MinValue

    // Numeric
    override def plus(x: UInt, y: UInt): UInt = x + y
    override def minus(x: UInt, y: UInt): UInt = x - y
    override def times(x: UInt, y: UInt): UInt = x * y
    override def negate(x: UInt): UInt = -x
    override def compare(x: UInt, y: UInt): Int = x.compare(y)

    // NumericOpsPlus
    override def fromString(value: String): UInt = UInt.trunc(value.toLong)

    override def toBytes(value: UInt): Array[Byte] = value.toBytes
    override def fromBytes(value: Array[Byte]): UInt = {
      val intValue = intOps.fromBytes(value)

      UInt.trunc(intValue)
    }

    override def toBoolean(value: UInt): Boolean = value.toBoolean

    override def toByte(value: UInt): Byte = value.toByte
    override def fromByte(value: Byte): UInt = UInt.trunc(value.toInt)

    override def toShort(value: UInt): Short = value.toShort
    override def fromShort(value: Short): UInt = UInt.trunc(value.toInt)

    override def toInt(value: UInt): Int = value.toInt
    override def fromInt(value: Int): UInt = UInt.trunc(value)

    override def toUInt(value: UInt): UInt = value
    override def fromUInt(value: UInt): UInt = value

    override def toLong(value: UInt): Long = value.toLong
    override def fromLong(value: Long): UInt = UInt.trunc(value)

    override def toFloat(value: UInt): Float = value.toFloat
    override def fromFloat(value: Float): UInt = UInt.trunc(value.toInt)

    override def toDouble(value: UInt): Double = value.toDouble
    override def fromDouble(value: Double): UInt = UInt.trunc(value.toLong)

    override def toBigInteger(value: UInt): BigInteger = value.toBigInteger
    override def fromBigInteger(value: BigInteger): UInt = UInt.trunc(value.longValue)

    override def toBigDecimal(value: UInt): java.math.BigDecimal = value.toBigDecimal
    override def fromBigDecimal(value: java.math.BigDecimal): UInt = UInt.trunc(value.longValue)

    // Mutator for Arrow Vectors
    override def setVector(vector: UInt4Vector, index: Int, value: UInt): Unit = {
      vector.setWithPossibleTruncate(index, value.toInt)
    }

    // Accessor for Arrow Vectors
    override def getVector(vector: UInt4Vector, index: Int): UInt = {
      this.fromInt(vector.get(index))
    }
  }

  implicit val longOps: NumericEncoder.Arrow[Long, BigIntVector] = new NumericEncoder.Arrow[Long, BigIntVector] {
    override val maxValue: Long = Long.MaxValue
    override val minValue: Long = Long.MinValue

    // Numeric
    override def plus(x: Long, y: Long): Long = x + y
    override def minus(x: Long, y: Long): Long = x - y
    override def times(x: Long, y: Long): Long = x * y
    override def negate(x: Long): Long = -x
    override def compare(x: Long, y: Long): Int = x.compare(y)

    // NumericOpsPlus
    override def fromString(value: String): Long = value.toLong

    override def toBytes(value: Long): Array[Byte] = Array(
      (value >> 56).toByte,
      (value >> 48).toByte,
      (value >> 40).toByte,
      (value >> 32).toByte,
      (value >> 24).toByte,
      (value >> 16).toByte,
      (value >> 8).toByte,
    )

    override def fromBytes(value: Array[Byte]): Long = {
      ((value(0) & 0xFF).toLong << 56) |
        ((value(1) & 0xFF).toLong << 48) |
        ((value(2) & 0xFF).toLong << 40) |
        ((value(3) & 0xFF).toLong << 32) |
        ((value(4) & 0xFF).toLong << 24) |
        ((value(5) & 0xFF).toLong << 16) |
        ((value(6) & 0xFF).toLong << 8) |
        (value(7) & 0xFF).toLong
    }

    override def toByte(value: Long): Byte = value.toByte
    override def fromByte(value: Byte): Long = value.toLong

    override def toShort(value: Long): Short = value.toShort
    override def fromShort(value: Short): Long = value.toLong

    override def toInt(value: Long): Int = value.toInt
    override def fromInt(value: Int): Long = value.toLong

    override def toLong(value: Long): Long = value
    override def fromLong(value: Long): Long = value

    override def toFloat(value: Long): Float = value.toFloat
    override def fromFloat(value: Float): Long = value.toLong

    override def toDouble(value: Long): Double = value.toDouble
    override def fromDouble(value: Double): Long = value.toLong

    override def toBigInteger(value: Long): BigInteger = BigInteger.valueOf(value)
    override def fromBigInteger(value: BigInteger): Long = value.longValue

    override def toBigDecimal(value: Long): java.math.BigDecimal = java.math.BigDecimal.valueOf(value)
    override def fromBigDecimal(value: java.math.BigDecimal): Long = value.longValue

    // Mutator for Arrow Vectors
    override def setVector(vector: BigIntVector, index: Int, value: Long): Unit = {
      vector.set(index, value)
    }

    // Accessor for Arrow Vectors
    override def getVector(vector: BigIntVector, index: Int): Long = {
      vector.get(index)
    }
  }

  implicit val ulongOps: NumericEncoder.Arrow[ULong, UInt8Vector] = new NumericEncoder.Arrow[ULong, UInt8Vector] {
    override val maxValue: ULong = ULong.MaxValue
    override val minValue: ULong = ULong.MinValue

    // Numeric
    override def plus(x: ULong, y: ULong): ULong = x + y
    override def minus(x: ULong, y: ULong): ULong = x - y
    override def times(x: ULong, y: ULong): ULong = x * y
    override def negate(x: ULong): ULong = -x
    override def compare(x: ULong, y: ULong): Int = x.compare(y)
    override def zero: ULong = ULong.Zero
    override def one: ULong = ULong.One

    // NumericOpsPlus
    override def fromString(value: String): ULong = ULong.trunc(BigInt(value))

    override def toBoolean(value: ULong): Boolean = value.toBoolean

    override def toBytes(value: ULong): Array[Byte] = value.toBytes
    override def fromBytes(value: Array[Byte]): ULong = {
      val longValue = longOps.fromBytes(value)

      ULong.trunc(longValue)
    }

    override def toByte(value: ULong): Byte = value.toByte
    override def fromByte(value: Byte): ULong = ULong.trunc(value.toLong)

    override def toShort(value: ULong): Short = value.toShort
    override def fromShort(value: Short): ULong = ULong.trunc(value.toLong)

    override def toInt(value: ULong): Int = value.toInt
    override def fromInt(value: Int): ULong = ULong.trunc(value.toLong)

    override def toLong(value: ULong): Long = value.toLong
    override def fromLong(value: Long): ULong = ULong.trunc(value)

    override def toULong(value: ULong): ULong = value
    override def fromULong(value: ULong): ULong = value

    override def toFloat(value: ULong): Float = value.toFloat
    override def fromFloat(value: Float): ULong = ULong.trunc(value.toLong)

    override def toDouble(value: ULong): Double = value.toDouble
    override def fromDouble(value: Double): ULong = ULong.trunc(value.toLong)

    override def toBigInteger(value: ULong): BigInteger = value.toBigInteger
    override def fromBigInteger(value: BigInteger): ULong = ULong.trunc(value)

    override def toBigDecimal(value: ULong): java.math.BigDecimal = value.toBigDecimal
    override def fromBigDecimal(value: java.math.BigDecimal): ULong = ULong.trunc(value.toBigInteger)

    // Mutator for Arrow Vectors
    override def setVector(vector: UInt8Vector, index: Int, value: ULong): Unit = {
      vector.set(index, value.toLong)
    }

    // Accessor for Arrow Vectors
    override def getVector(vector: UInt8Vector, index: Int): ULong = {
      this.fromLong(vector.get(index))
    }
  }
}


package io.github.platob.arrow4s.core.cast

import io.github.platob.arrow4s.core.reflection.ReflectUtils
import io.github.platob.arrow4s.core.values.{UByte, UInt, ULong, UShort}
import org.apache.arrow.vector.FieldVector

import scala.reflect.runtime.{universe => ru}

trait AnyEncoder[T] extends Numeric[T] {
  @inline def toString(value: T): String = value.toString
  @inline def fromString(value: String): T

  @inline override def parseString(str: String): Option[T] = {
    try {
      Some(fromString(str))
    } catch {
      case _: Throwable => None
    }
  }

  @inline def toBytes(value: T): Array[Byte]
  @inline def fromBytes(value: Array[Byte]): T

  @inline def toBoolean(value: T): Boolean
  @inline def fromBoolean(value: Boolean): T

  @inline def toByte(value: T): Byte
  @inline def fromByte(value: Byte): T

  @inline def toUByte(value: T): UByte = UByte.trunc(toInt(value))
  @inline def fromUByte(value: UByte): T = fromInt(value.toInt)

  @inline def toShort(value: T): Short
  @inline def fromShort(value: Short): T

  @inline def toUShort(value: T): UShort = UShort.trunc(toInt(value))
  @inline def fromUShort(value: UShort): T = fromInt(value.toInt)

  @inline def toInt(value: T): Int
  @inline def fromInt(value: Int): T

  @inline def toUInt(value: T): UInt = UInt.trunc(toLong(value))
  @inline def fromUInt(value: UInt): T = fromLong(value.toLong)

  @inline def toLong(value: T): Long
  @inline def fromLong(value: Long): T

  @inline def toULong(value: T): ULong = ULong.trunc(toBigInteger(value))
  @inline def fromULong(value: ULong): T = fromBigInteger(value.toBigInteger)

  @inline def toFloat(value: T): Float
  @inline def fromFloat(value: Float): T

  @inline def toDouble(value: T): Double
  @inline def fromDouble(value: Double): T

  @inline def toBigInteger(value: T): java.math.BigInteger
  @inline def fromBigInteger(value: java.math.BigInteger): T

  @inline def toBigDecimal(value: T): java.math.BigDecimal
  @inline def fromBigDecimal(value: java.math.BigDecimal): T

  @inline def toInstant(value: T): java.time.Instant
  @inline def fromInstant(value: java.time.Instant): T
}

object AnyEncoder {
  trait Arrow[T, V <: FieldVector] extends AnyEncoder[T] {
    // Mutator for Arrow Vectors
    @inline def setVector(vector: V, index: Int, value: T): Unit
    @inline def setVectorNull(vector: V, index: Int): Unit = {
      vector.setNull(index)
    }
    @inline def setVectorOption(vector: V, index: Int, value: Option[T]): Unit = {
      value match {
        case Some(v) => setVector(vector, index, v)
        case None    => setVectorNull(vector, index)
      }
    }
    @inline def setAnyVector(vector: FieldVector, index: Int, value: T): Unit = {
      setVector(vector.asInstanceOf[V], index, value)
    }

    // Accessor for Arrow Vectors
    @inline def getVector(vector: V, index: Int): T
    @inline def getVectorIsNull(vector: V, index: Int): Boolean = {
      vector.isNull(index)
    }
    @inline def getVectorOption(vector: V, index: Int): Option[T] = {
      if (getVectorIsNull(vector, index)) None else Some(getVector(vector, index))
    }
    @inline def getAnyVector(vector: FieldVector, index: Int): T = {
      getVector(vector.asInstanceOf[V], index)
    }
  }

  def findType[T : ru.TypeTag]: AnyEncoder.Arrow[T, _] = {
    val tpe = ru.typeOf[T]

    findType(tpe).asInstanceOf[AnyEncoder.Arrow[T, _]]
  }

  def findType(tpe: ru.Type): AnyEncoder.Arrow[_, _] = tpe match {
    case opt if ReflectUtils.isOption(opt) =>
      val innerType = ReflectUtils.getTypeArgs(opt).head

      findType(innerType) match {
        case base: AnyEncoder.Arrow[t, v] @unchecked =>
          LogicalEncoder.optionalArrow[t, v](base)
        case _ =>
          throw new IllegalArgumentException(s"No ArrowEncoder found for type $innerType")
      }
    case t if t =:= ru.typeOf[Byte]   => Implicits.byteOps
    case t if t =:= ru.typeOf[UByte]  => Implicits.ubyteOps
    case t if t =:= ru.typeOf[Short]  => Implicits.shortOps
    case t if t =:= ru.typeOf[UShort] => Implicits.ushortOps
    case t if t =:= ru.typeOf[Int]    => Implicits.intOps
    case t if t =:= ru.typeOf[UInt]   => Implicits.uintOps
    case t if t =:= ru.typeOf[Long]   => Implicits.longOps
    case t if t =:= ru.typeOf[ULong]  => Implicits.ulongOps
    case _ =>
      throw new IllegalArgumentException(s"No ArrowEncoder found for type $tpe")
  }
}

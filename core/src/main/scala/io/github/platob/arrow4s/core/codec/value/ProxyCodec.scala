package io.github.platob.arrow4s.core.codec.value

import org.apache.arrow.vector.types.pojo.Field

import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}

abstract class ProxyCodec[In, Out](
  val arrowField: Field,
  val inner: ValueCodec[In],
  val tpe: ru.Type,
  val typeTag: ru.TypeTag[Out],
  val clsTag: ClassTag[Out],
) extends ValueCodec[Out] {
  def encode(value: Out): In
  def decode(value: In): Out

  override def namespace: String = s"${clsTag.runtimeClass.getSimpleName}[${inner.namespace}]"
  override def one: Out = decode(inner.one)
  override def zero: Out = decode(inner.zero)
  override def default: Out = decode(inner.default)
  override def bitSize: Int = inner.bitSize
  override def isTuple: Boolean = inner.isTuple
  override def isPrimitive: Boolean = inner.isPrimitive

  override def children: Seq[ValueCodec[_]] = Seq(inner)

  override def elementAt[E](value: Out, index: Int): E = inner.elementAt[E](encode(value), index)

  override def elements(value: Out): Array[Any] = inner.elements(encode(value))

  override def fromElements(values: Array[Any]): Out = decode(inner.fromElements(values))

  override def isNull(value: Out): Boolean = inner.isNull(encode(value))

  override def toBytes(value: Out): Array[Byte] = inner.toBytes(encode(value))
  override def fromBytes(value: Array[Byte]): Out = decode(inner.fromBytes(value))

  override def toString(value: Out, charset: java.nio.charset.Charset): String = inner.toString(encode(value), charset)
  override def fromString(value: String, charset: java.nio.charset.Charset): Out = decode(inner.fromString(value, charset))
}

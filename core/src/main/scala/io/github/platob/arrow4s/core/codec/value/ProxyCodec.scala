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
  def decode(value: Out): In
  def encode(value: In): Out

  override def namespace: String = s"${clsTag.runtimeClass.getSimpleName}[${inner.namespace}]"
  override def one: Out = encode(inner.one)
  override def zero: Out = encode(inner.zero)
  override def default: Out = encode(inner.default)
  override def bitSize: Int = inner.bitSize
  override def isTuple: Boolean = inner.isTuple
  override def isPrimitive: Boolean = inner.isPrimitive

  override def children: Seq[ValueCodec[_]] = Seq(inner)

  override def elementAt[E](value: Out, index: Int): E = inner.elementAt[E](decode(value), index)

  override def elements(value: Out): Array[Any] = inner.elements(decode(value))

  override def fromElements(values: Array[Any]): Out = encode(inner.fromElements(values))

  override def isNull(value: Out): Boolean = inner.isNull(decode(value))
}

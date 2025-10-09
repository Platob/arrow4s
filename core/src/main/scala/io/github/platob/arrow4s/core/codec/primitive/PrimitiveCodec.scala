package io.github.platob.arrow4s.core.codec.primitive

import io.github.platob.arrow4s.core.codec.{ReflectedCodec, ValueCodec}

import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}

abstract class PrimitiveCodec[T : ru.TypeTag : ClassTag] extends ReflectedCodec[T] {
  override def toString: String = s"PrimitiveCodec[$namespace]"

  def default: T = zero

  override def elementAt[E](value: T, index: Int): E =
    if (index == 0) value.asInstanceOf[E]
    else throw new IndexOutOfBoundsException(s"Primitive type $tpe has no element at index $index")

  override def elements(value: T): Array[Any] = Array(value)

  override def fromElements(values: Array[Any]): T =
    if (values.isEmpty) default
    else unsafeAs(values(0))

  override def children: Seq[ValueCodec[_]] = Seq.empty
}

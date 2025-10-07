package io.github.platob.arrow4s.core.arrays.nested

import io.github.platob.arrow4s.core.arrays.ArrowArray
import io.github.platob.arrow4s.core.reflection.ReflectUtils
import org.apache.arrow.vector.ValueVector

import scala.reflect.ClassTag

trait NestedArray extends ArrowArray {
  override def isPrimitive: Boolean = false

  override def isNested: Boolean = true

  override def isOptional: Boolean = false

  override def isLogical: Boolean = false
}

object NestedArray {
  abstract class Typed[V <: ValueVector, T]
    extends ArrowArray.Typed[V, T] with NestedArray {

    implicit lazy val classTag: ClassTag[T] = ReflectUtils.classTag[T](scalaType)

    override def toArray(start: Int, end: Int): Array[T] = {
      (start until end).map(get).toArray
    }
  }
}

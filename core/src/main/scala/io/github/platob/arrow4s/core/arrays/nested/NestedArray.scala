package io.github.platob.arrow4s.core.arrays.nested

import io.github.platob.arrow4s.core.arrays.ArrowArray
import org.apache.arrow.vector.ValueVector
import org.apache.arrow.vector.types.pojo.Field

trait NestedArray[T] extends ArrowArray[T] {
  override def isPrimitive: Boolean = false

  override def isNested: Boolean = true

  override def isOptional: Boolean = false

  override def isLogical: Boolean = false

  def childrenArrays: Seq[ArrowArray[_]]

  def childFields: Seq[Field] = childrenArrays.map(_.field)

  def child(index: Int): ArrowArray[_] = childrenArrays(index)

  def child(name: String): ArrowArray[_] = {
    var index = childFields.indexWhere(_.getName == name)

    if (index == -1) {
      // Try case-insensitive match
      index = childFields.indexWhere(_.getName.equalsIgnoreCase(name))

      if (index == -1)
        throw new NoSuchElementException(s"'$name' not found in fields: ${childFields.map(_.getName).mkString("'", "', '", "'")}")
    }

    childrenArrays(index)
  }
}

object NestedArray {
  abstract class Typed[V <: ValueVector, T]
    extends ArrowArray.Typed[V, T] with NestedArray[T] {

  }
}

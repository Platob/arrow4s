package io.github.platob.arrow4s.core.arrays.nested

import io.github.platob.arrow4s.core.arrays.ArrowArray
import org.apache.arrow.vector.ValueVector

import scala.reflect.runtime.{universe => ru}

trait NestedArray extends ArrowArray {
  override def isPrimitive: Boolean = false

  override def isNested: Boolean = true

  override def isOptional: Boolean = false

  override def isLogical: Boolean = false

  private lazy val childrenArrays: Array[Option[ArrowArray]] = Array.fill(cardinality)(None)

  @inline override def childArray(index: Int): ArrowArray = {
    val cached = childrenArrays(index)

    cached.getOrElse {
      val childVector = this.childVector(index)
      val childArray = ArrowArray.from(childVector)

      childrenArrays(index) = Some(childArray)

      childArray
    }
  }
}

object NestedArray {
  abstract class Typed[V <: ValueVector, T : ru.TypeTag]
    extends ArrowArray.Typed[V, T] with NestedArray {

  }
}

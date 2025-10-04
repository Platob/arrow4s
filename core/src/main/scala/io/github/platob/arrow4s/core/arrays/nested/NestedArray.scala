package io.github.platob.arrow4s.core.arrays.nested

import io.github.platob.arrow4s.core.arrays.ArrowArray
import org.apache.arrow.vector.ValueVector

import scala.reflect.runtime.{universe => ru}

trait NestedArray extends ArrowArray {
  override def isPrimitive: Boolean = false

  override def isNested: Boolean = true

  override def isOptional: Boolean = false

  override def isLogical: Boolean = false

  private lazy val childrenArrays: Seq[ArrowArray] = (0 until this.cardinality).map(i => {
    val v = this.childVector(i)

    ArrowArray.from(v)
  })

  @inline override def childArray(index: Int): ArrowArray = childrenArrays(index)
}

object NestedArray {
  abstract class Typed[V <: ValueVector, T : ru.TypeTag]
    extends ArrowArray.Typed[V, T] with NestedArray {

  }
}

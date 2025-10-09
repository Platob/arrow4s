package io.github.platob.arrow4s.core.arrays.nested

import io.github.platob.arrow4s.core.arrays.ArrowArray
import io.github.platob.arrow4s.core.arrays.traits.TArrowArray
import io.github.platob.arrow4s.core.codec.ValueCodec
import org.apache.arrow.vector.ValueVector

trait NestedArray[T] extends ArrowArray[T] with NestedArray.TNestedArray {

}

object NestedArray {
  trait TNestedArray extends TArrowArray {
    override def isPrimitive: Boolean = false

    override def isNested: Boolean = true

    override def isOptional: Boolean = false

    override def isLogical: Boolean = false
  }

  abstract class Typed[Value, ArrowVector <: ValueVector, Arr <: Typed[Value, ArrowVector, Arr]](
    val vector: ArrowVector
  ) extends ArrowArray.Typed[Value, ArrowVector, Arr] with NestedArray[Value] {
    lazy val codec: ValueCodec[Value] = ValueCodec
      .fromField(arrowField = vector.getField)
      .asInstanceOf[ValueCodec[Value]]
  }
}

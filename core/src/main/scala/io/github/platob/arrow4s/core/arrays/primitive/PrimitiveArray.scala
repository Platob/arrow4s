package io.github.platob.arrow4s.core.arrays.primitive

import io.github.platob.arrow4s.core.arrays.traits.TPrimitiveArray
import io.github.platob.arrow4s.core.arrays.{ArrowArray, LogicalArray}
import io.github.platob.arrow4s.core.codec.ValueCodec
import org.apache.arrow.vector.FieldVector

trait PrimitiveArray[T] extends ArrowArray[T] with TPrimitiveArray {

}

object PrimitiveArray {
  abstract class Typed[Value : ValueCodec, ArrowVector <: FieldVector, Arr <: Typed[Value, ArrowVector, Arr]](
    val vector: ArrowVector
  ) extends ArrowArray.Typed[Value, ArrowVector, Arr] with PrimitiveArray[Value] {
    val codec: ValueCodec[Value] = implicitly[ValueCodec[Value]]

    override def children: Seq[ArrowArray.Typed[_, _, _]] = Seq.empty

    override def setNull(index: Int): this.type = {
      vector.setNull(index)

      this
    }

    // Cast
    override def innerAs(codec: ValueCodec[_]): ArrowArray[_] = {
      codec match {
        case c: ValueCodec[v] @unchecked =>
          LogicalArray.convertPrimitive[Value, ArrowVector, Arr](this.asInstanceOf[Arr], c)
      }
    }
  }
}

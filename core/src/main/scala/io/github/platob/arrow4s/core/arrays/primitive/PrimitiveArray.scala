package io.github.platob.arrow4s.core.arrays.primitive

import io.github.platob.arrow4s.core.arrays.{ArrowArray, LogicalArray}
import io.github.platob.arrow4s.core.extensions.TypeExtension
import org.apache.arrow.vector.FieldVector

import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}

trait PrimitiveArray extends ArrowArray {
  override def isPrimitive: Boolean = true

  override def isNested: Boolean = false

  override def isOptional: Boolean = false

  override def isLogical: Boolean = false
}

object PrimitiveArray {
  abstract class Typed[V <: FieldVector, T : TypeExtension]
    extends ArrowArray.Typed[V, T] with PrimitiveArray {
    val typeExtension: TypeExtension[T] = implicitly[TypeExtension[T]]

    override def scalaType: ru.Type = typeExtension.tpe

    implicit val classTag: ClassTag[T] = typeExtension.classTag

    override def children: Seq[ArrowArray.Typed[_, _]] = Seq.empty

    override def setNull(index: Int): this.type = {
      vector.setNull(index)

      this
    }

    // Cast
    override def innerAs(tpe: ru.Type): ArrowArray.Typed[V, _] = {
      LogicalArray.convertPrimitive[PrimitiveArray.Typed[V, T], V, T](arr = this, tpe = tpe)
        .asInstanceOf[ArrowArray.Typed[V, _]]
    }

    override def toArray(start: Int, end: Int): Array[T] = {
      (start until end).map(get).toArray
    }
  }
}

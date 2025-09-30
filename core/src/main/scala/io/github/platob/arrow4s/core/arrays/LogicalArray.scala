package io.github.platob.arrow4s.core.arrays

import io.github.platob.arrow4s.core.cast.TypeConverter
import org.apache.arrow.vector.FieldVector

import scala.reflect.runtime.{universe => ru}

class LogicalArray[V <: FieldVector, Source, Target](
  val scalaType: ru.Type,
  val array: ArrowArray.Typed[V, Source],
  val converter: TypeConverter[Source, Target],
) extends ArrowArray.Typed[V, Target] {
  override def vector: V = array.vector

  override def isPrimitive: Boolean = array.isPrimitive

  override def isOptional: Boolean = true

  override def isLogical: Boolean = true

  // Accessors
  override def get(index: Int): Target = {
    val sourceValue = array.get(index)
    converter.to(sourceValue)
  }

  // Mutators
  override def set(index: Int, value: Target): this.type = {
    val sourceValue = converter.from(value)
    array.set(index, sourceValue)

    this
  }

  override def as(castTo: ru.Type): ArrowArray = {
    if (this.scalaType =:= castTo)
      return this

    if (this.array.scalaType =:= castTo)
      return this.array

    this.array.as(castTo)
  }

  override def close(): Unit = array.close()
}

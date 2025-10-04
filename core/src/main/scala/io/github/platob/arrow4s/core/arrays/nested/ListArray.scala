package io.github.platob.arrow4s.core.arrays.nested

import io.github.platob.arrow4s.core.arrays.ArrowArray
import org.apache.arrow.vector.ValueVector
import org.apache.arrow.vector.complex.ListVector

import scala.reflect.runtime.{universe => ru}

class ListArray(val vector: ListVector) extends NestedArray.Typed[ListVector, ArrowArray] {
  override val scalaType: ru.Type = ru.typeOf[ArrowArray]

  private val elements: ArrowArray = ArrowArray.from(this.vector.getDataVector)

  override def childVector(index: Int): ValueVector = this.vector.getDataVector

  override def isNull(index: Int): Boolean = this.vector.isNull(index)

  override def get(index: Int): ArrowArray = {
    val (start, end) = (
      this.vector.getElementStartIndex(index),
      this.vector.getElementEndIndex(index)
    )

    elements.slice(start, end)
  }

  override def setNull(index: Int): this.type = {
    this.vector.setNull(index)
    this
  }

  override def set(index: Int, value: ArrowArray): this.type = {
    val (start, end) = (
      this.vector.getElementStartIndex(index),
      this.vector.getElementEndIndex(index)
    )

    for (i <- 0 until value.length) {
      elements.setAnyOrNull(start + i, value.getAnyOrNull(i))
    }

    this
  }
}

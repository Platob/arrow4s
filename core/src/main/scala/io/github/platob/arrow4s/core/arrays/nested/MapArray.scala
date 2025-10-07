package io.github.platob.arrow4s.core.arrays.nested

import io.github.platob.arrow4s.core.arrays.ArrowArray
import org.apache.arrow.vector.complex.MapVector

import scala.reflect.runtime.{universe => ru}

class MapArray[Key, Value](
  val scalaType: ru.Type,
  val vector: MapVector,
  val elements: StructArray[(Key, Value)]
) extends NestedArray.Typed[MapVector, scala.collection.Map[Key, Value]] {
  override def childrenArrays: Seq[ArrowArray[_]] = Seq(elements)

  override def get(index: Int): scala.collection.Map[Key, Value] = {
    val (start, end) = (
      this.vector.getElementStartIndex(index),
      this.vector.getElementEndIndex(index)
    )

    val items = elements.slice(start, end)

    items.map { case (k, v) => (k, v) }.toMap
  }

  override def setNull(index: Int): this.type = {
    this.vector.setNull(index)
    this
  }

  override def set(index: Int, value: scala.collection.Map[Key, Value]): this.type = {
    if (value == null) {
      setNull(index)
    } else {
      this.vector.startNewValue(index)

      value.foreach { case (k, v) =>
        val elemIndex = elements.cardinality
        elements.set(elemIndex, (k, v))
        this.vector.endValue(index, elemIndex + 1)
      }

      this.vector.endValue(index, value.size)
    }

    this
  }
}

object MapArray {
  def default(vector: MapVector): MapArray[Any, Any] = {
    val tpe = ru.typeOf[scala.collection.Map[Any, Any]]
    val elements = ArrowArray.from(vector.getDataVector).asInstanceOf[StructArray[(Any, Any)]]

    new MapArray[Any, Any](
      scalaType = tpe,
      vector = vector,
      elements = elements
    )
  }
}

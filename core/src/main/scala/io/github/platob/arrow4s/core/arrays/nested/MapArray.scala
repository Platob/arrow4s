package io.github.platob.arrow4s.core.arrays.nested

import io.github.platob.arrow4s.core.arrays.ArrowArray
import org.apache.arrow.vector.complex.MapVector

import scala.reflect.runtime.{universe => ru}

class MapArray[Key, Value](
  val scalaType: ru.Type,
  val vector: MapVector,
  val elements: ArrowArray.Typed[_, (Key, Value)]
) extends NestedArray.Typed[MapVector, scala.collection.Map[Key, Value]] {
  override def children: Seq[ArrowArray.Typed[_, _]] = Seq(elements)

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

  override def toArray(start: Int, size: Int): Array[scala.collection.Map[Key, Value]] = {
    (start until (start + size)).map(get).toArray
  }
}

object MapArray {
  def default(vector: MapVector): MapArray[Any, Any] = {
    val tpe = ru.typeOf[scala.collection.Map[Any, Any]]
    val elements = ArrowArray.from(vector.getDataVector)
      .asInstanceOf[ArrowArray.Typed[_, (Any, Any)]]

    new MapArray[Any, Any](
      scalaType = tpe,
      vector = vector,
      elements = elements
    )
  }
}

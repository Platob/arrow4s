package io.github.platob.arrow4s.core.arrays.nested

import io.github.platob.arrow4s.core.arrays.{ArraySlice, ArrowArray, LogicalArray}
import io.github.platob.arrow4s.core.reflection.ReflectUtils
import org.apache.arrow.vector.complex.ListVector

import scala.reflect.runtime.{universe => ru}

class ListArray[ElementType](
  val scalaType: ru.Type,
  val vector: ListVector,
  val elements: ArrowArray.Typed[_, ElementType]
) extends NestedArray.Typed[ListVector, Iterable[ElementType]] {
  override def children: Seq[ArrowArray.Typed[_, ElementType]] = Seq(elements)

  override def innerAs(tpe: ru.Type): ArrowArray.Typed[ListVector, _] = {
    if (!ReflectUtils.isIterable(tpe)) {
      throw new IllegalArgumentException(s"Type $tpe is not a Seq-like type")
    }

    val elemType = ReflectUtils.typeArgument(tpe, 0)

    elements.as(elemType) match {
      case arr: ArrowArray.Typed[v, e] =>
        val casted = new ListArray[e](
          scalaType = tpe,
          vector = this.vector,
          elements = arr
        )

        // Check the collection type conversions
        val targetCollectionType = tpe.typeConstructor

        if (targetCollectionType =:= ru.typeOf[List[_]].typeConstructor) {
          LogicalArray.instance[ListArray[e], ListVector, Iterable[e], List[e]](
            array = casted,
            toLogical = (v: Iterable[e]) => v.toList,
            toPhysical = (v: List[e]) => v,
            tpe = tpe
          )
        }
        else if (targetCollectionType =:= ru.typeOf[Vector[_]].typeConstructor) {
          val getter = (arr: LogicalArray[ListArray[e], ListVector, Iterable[e], Vector[e]], index: Int) => {
            val v = arr.inner.get(index)
            v.toVector
          }
          val setter = (arr: LogicalArray[ListArray[e], ListVector, Iterable[e], Vector[e]], index: Int, value: Vector[e]) => {
            arr.inner.set(index, value.toSeq)
          }

          new LogicalArray[ListArray[e], ListVector, Iterable[e], Vector[e]](
            scalaType = tpe,
            inner = casted,
            getter = getter,
            setter = setter,
            children = Seq(arr)
          )
        }
        else if (targetCollectionType =:= ru.typeOf[Array[_]].typeConstructor) {
          val getter = (arr: LogicalArray[ListArray[e], ListVector, Iterable[e], Array[e]], index: Int) => {
            val v = arr.inner.get(index)
            v.toArray
          }
          val setter = (arr: LogicalArray[ListArray[e], ListVector, Iterable[e], Array[e]], index: Int, value: Array[e]) => {
            arr.inner.set(index, value.toSeq)
          }

          new LogicalArray[ListArray[e], ListVector, Iterable[e], Array[e]](
            scalaType = tpe,
            inner = casted,
            getter = getter,
            setter = setter,
            children = Seq(arr)
          )
        }
        else {
          // Return as is, Seq trait is enough
          casted
        }
    }
  }

  override def get(index: Int): ArraySlice.Typed[_, ElementType] = {
    val (start, end) = (
      this.vector.getElementStartIndex(index),
      this.vector.getElementEndIndex(index)
    )

    val items = elements.slice(start, end)

    items
  }

  override def setNull(index: Int): this.type = {
    this.vector.setNull(index)
    this
  }

  override def set(index: Int, value: Iterable[ElementType]): this.type = {
    vector.startNewValue(index)

    elements.setValues(index = vector.getElementStartIndex(index), value)

    vector.endValue(index, value.size)

    this
  }

  override def toArray(start: Int, size: Int): Array[Iterable[ElementType]] = {
    (start until (start + size)).map(get).toArray
  }
}

object ListArray {
  def default(vector: ListVector): ListArray[Any] = {
    val tpe = ru.typeOf[scala.collection.Seq[Any]]
    val elements = ArrowArray.from(vector.getDataVector).asInstanceOf[ArrowArray.Typed[_, Any]]

    new ListArray[Any](
      scalaType = tpe,
      vector = vector,
      elements = elements
    )
  }
}
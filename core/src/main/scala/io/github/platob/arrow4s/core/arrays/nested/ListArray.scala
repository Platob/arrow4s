package io.github.platob.arrow4s.core.arrays.nested

import io.github.platob.arrow4s.core.arrays.{ArraySlice, ArrowArray, LogicalArray}
import io.github.platob.arrow4s.core.reflection.ReflectUtils
import org.apache.arrow.vector.complex.ListVector

import scala.reflect.runtime.{universe => ru}

class ListArray[V <: ListVector, ElementType](
  val scalaType: ru.Type,
  val vector: V,
  val elements: ArrowArray.Typed[_, ElementType]
) extends NestedArray.Typed[V, ArrowArray.Typed[_, ElementType]] {
  override def children: Seq[ArrowArray.Typed[_, ElementType]] = Seq(elements)

  /**
   * Get the start and end indices of the elements for the given index.
   * @param index the index of the list element
   * @return
   */
  @inline def getStartEnd(index: Int): (Int, Int) = {
    (
      vector.getElementStartIndex(index),
      vector.getElementEndIndex(index)
    )
  }

  override def innerAs(tpe: ru.Type): ArrowArray.Typed[V, _] = {
    if (!ReflectUtils.isIterable(tpe)) {
      throw new IllegalArgumentException(s"Type $tpe is not a iterable type")
    }

    val elemType = ReflectUtils.typeArgument(tpe, 0)

    elements.as(elemType) match {
      case casted: ArrowArray.Typed[_, e] =>
        // Check the collection type conversions
        val targetCollectionType = tpe.typeConstructor

        if (targetCollectionType =:= ru.typeOf[Array[_]].typeConstructor) {
          val getter = (arr: LogicalArray[ListArray[V, ElementType], V, ArrowArray.Typed[_, ElementType], Array[e]], index: Int) => {
            val (start, end) = (
              arr.vector.getElementStartIndex(index),
              arr.vector.getElementEndIndex(index)
            )

            casted.slice(start = start, end = end).toArray
          }

          val setter = (arr: LogicalArray[ListArray[V, ElementType], V, ArrowArray.Typed[_, ElementType], Array[e]], index: Int, value: Array[e]) => {
            arr.vector.startNewValue(index)

            casted.setValues(index = arr.vector.getElementStartIndex(index), value)

            arr.vector.endValue(index, value.length)

            ()
          }

          new LogicalArray[ListArray[V, ElementType], V, ArrowArray.Typed[_, ElementType], Array[e]](
            scalaType = tpe,
            inner = this,
            getter = getter,
            setter = setter,
            children = Seq(casted)
          )
        }
        else if (targetCollectionType =:= ru.typeOf[collection.immutable.Seq[_]].typeConstructor) {
          val getter = (
            arr: LogicalArray[ListArray[V, ElementType], V, ArrowArray.Typed[_, ElementType], collection.immutable.Seq[e]],
            index: Int
          ) => {
            val (start, end) = (
              arr.vector.getElementStartIndex(index),
              arr.vector.getElementEndIndex(index)
            )

            casted.slice(start = start, end = end)
          }

          val setter = (
            arr: LogicalArray[ListArray[V, ElementType], V, ArrowArray.Typed[_, ElementType], collection.immutable.Seq[e]],
            index: Int, value: Iterable[e]
          ) => {
            arr.vector.startNewValue(index)

            casted.setValues(index = arr.vector.getElementStartIndex(index), value)

            arr.vector.endValue(index, value.size)

            ()
          }

          new LogicalArray[ListArray[V, ElementType], V, ArrowArray.Typed[_, ElementType], collection.immutable.Seq[e]](
            scalaType = tpe,
            inner = this,
            getter = getter,
            setter = setter,
            children = Seq(casted)
          )
        }
        else if (targetCollectionType =:= ru.typeOf[collection.immutable.List[_]].typeConstructor) {
          val getter = (
            arr: LogicalArray[ListArray[V, ElementType], V, ArrowArray.Typed[_, ElementType], collection.immutable.List[e]],
            index: Int
          ) => {
            val (start, end) = (
              arr.vector.getElementStartIndex(index),
              arr.vector.getElementEndIndex(index)
            )

            collection.immutable.List.from(casted.slice(start = start, end = end))
          }

          val setter = (
            arr: LogicalArray[ListArray[V, ElementType], V, ArrowArray.Typed[_, ElementType], collection.immutable.List[e]],
            index: Int, value: Iterable[e]
          ) => {
            arr.vector.startNewValue(index)

            casted.setValues(index = arr.vector.getElementStartIndex(index), value)

            arr.vector.endValue(index, value.size)

            ()
          }

          new LogicalArray[ListArray[V, ElementType], V, ArrowArray.Typed[_, ElementType], collection.immutable.List[e]](
            scalaType = tpe,
            inner = this,
            getter = getter,
            setter = setter,
            children = Seq(casted)
          )
        }
        else if (targetCollectionType =:= ru.typeOf[collection.Seq[_]].typeConstructor) {
          val getter = (
            arr: LogicalArray[ListArray[V, ElementType], V, ArrowArray.Typed[_, ElementType], collection.Seq[e]],
            index: Int
          ) => {
            val (start, end) = (
              arr.vector.getElementStartIndex(index),
              arr.vector.getElementEndIndex(index)
            )

            collection.Seq.from(casted.slice(start = start, end = end))
          }

          val setter = (
            arr: LogicalArray[ListArray[V, ElementType], V, ArrowArray.Typed[_, ElementType], collection.Seq[e]],
            index: Int, value: Iterable[e]
          ) => {
            arr.vector.startNewValue(index)

            casted.setValues(index = arr.vector.getElementStartIndex(index), value)

            arr.vector.endValue(index, value.size)

            ()
          }

          new LogicalArray[ListArray[V, ElementType], V, ArrowArray.Typed[_, ElementType], collection.Seq[e]](
            scalaType = tpe,
            inner = this,
            getter = getter,
            setter = setter,
            children = Seq(casted)
          )
        }
        else {
          throw new IllegalArgumentException(s"Unsupported collection type: $targetCollectionType")
        }
    }
  }

  override def get(index: Int): ArraySlice.Typed[_, ElementType] = {
    val (start, end) = getStartEnd(index)

    val items = elements.slice(start = start, end = end)

    items
  }

  @inline def getArray(index: Int): Array[ElementType] = {
    val (start, end) = getStartEnd(index)

    elements.toArray(start, end - start)
  }

  @inline def getIterable(index: Int): Iterable[ElementType] = {
    getArray(index)
  }

  override def setNull(index: Int): this.type = {
    this.vector.setNull(index)
    this
  }

  override def set(index: Int, value: ArrowArray.Typed[_, ElementType]): this.type = {
    setIterable(index, value)
  }

  @inline def setArray(index: Int, value: Array[ElementType]): this.type = {
    vector.startNewValue(index)

    elements.setValues(index = vector.getElementStartIndex(index), value)

    vector.endValue(index, value.length)

    this
  }

  @inline def setIterable(index: Int, value: Iterable[ElementType]): this.type = {
    vector.startNewValue(index)

    elements.setValues(index = vector.getElementStartIndex(index), value)

    vector.endValue(index, value.size)

    this
  }

  override def toArray(start: Int, end: Int): Array[ArrowArray.Typed[_, ElementType]] = {
    (start until end).map(get).toArray
  }
}

object ListArray {
  def default(vector: ListVector): ListArray[ListVector, Any] = {
    val elements = ArrowArray.from(vector.getDataVector).asInstanceOf[ArrowArray.Typed[_, Any]]

    new ListArray[ListVector, Any](
      scalaType = ru.typeOf[Iterable[Any]],
      vector = vector,
      elements = elements
    )
  }
}
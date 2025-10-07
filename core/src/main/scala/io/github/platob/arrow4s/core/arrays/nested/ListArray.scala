package io.github.platob.arrow4s.core.arrays.nested

import io.github.platob.arrow4s.core.arrays.{ArraySlice, ArrowArray, LogicalArray}
import io.github.platob.arrow4s.core.reflection.ReflectUtils
import org.apache.arrow.vector.complex.ListVector

import scala.reflect.runtime.{universe => ru}

class ListArray[ElementType](
  val scalaType: ru.Type,
  val vector: ListVector,
  val elements: ArrowArray[ElementType]
) extends NestedArray.Typed[ListVector, scala.collection.immutable.Seq[ElementType]] {
  override def childrenArrays: Seq[ArrowArray[ElementType]] = Seq(elements)

  override def innerAs(tpe: ru.Type): ArrowArray[_] = {
    if (!ReflectUtils.isCollection(tpe)) {
      throw new IllegalArgumentException(s"Type $tpe is not a Seq-like type")
    }

    val elemType = ReflectUtils.typeArgument(tpe, 0)

    elements.as(elemType) match {
      case arr: ArrowArray[e] @unchecked =>
        val casted = new ListArray[e](
          scalaType = tpe,
          vector = this.vector,
          elements = arr
        )

        // Check the collection type conversions
        val targetCollectionType = tpe.typeConstructor

        if (targetCollectionType =:= ru.typeOf[List[_]].typeConstructor) {
          LogicalArray.instance[ListVector, Seq[e], List[e]](
            array = casted,
            toLogical = (v: Seq[e]) => v.toList,
            toPhysical = (v: List[e]) => v,
            tpe = tpe
          )
        }
        else if (targetCollectionType =:= ru.typeOf[Vector[_]].typeConstructor) {
          LogicalArray.instance[ListVector, Seq[e], Vector[e]](
            array = casted,
            toLogical = (v: Seq[e]) => v.toVector,
            toPhysical = (v: Vector[e]) => v,
            tpe = tpe
          )
        }
        else if (targetCollectionType =:= ru.typeOf[Array[_]].typeConstructor) {
          LogicalArray.instance[ListVector, Seq[e], Array[e]](
            array = casted,
            // build an Array with the exact component type, then cast to Array[e]
            // TODO: this might be inefficient for large arrays; consider caching the Class[_] for e
            toLogical = (v: Seq[e]) => ???,
            // you already have a correctly-typed Array[e], so identity is fine
            toPhysical = (v: Array[e]) => v,
            tpe = tpe
          )
        }
        else {
          // Return as is, Seq trait is enough
          casted
        }
    }
  }

  override def get(index: Int): ArraySlice[ElementType] = {
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

  override def set(index: Int, value: scala.collection.immutable.Seq[ElementType]): this.type = {
    vector.startNewValue(index)

    elements.setValues(index = vector.getElementStartIndex(index), value)

    vector.endValue(index, value.size)

    this
  }
}

object ListArray {
  def default(vector: ListVector): ListArray[Any] = {
    val tpe = ru.typeOf[scala.collection.Seq[Any]]
    val elements = ArrowArray.from(vector.getDataVector).asInstanceOf[ArrowArray[Any]]

    new ListArray[Any](
      scalaType = tpe,
      vector = vector,
      elements = elements
    )
  }
}
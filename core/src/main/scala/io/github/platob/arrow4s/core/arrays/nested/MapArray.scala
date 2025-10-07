package io.github.platob.arrow4s.core.arrays.nested

import io.github.platob.arrow4s.core.arrays.{ArrowArray, LogicalArray}
import io.github.platob.arrow4s.core.reflection.ReflectUtils
import org.apache.arrow.vector.complex.MapVector

import scala.reflect.runtime.{universe => ru}

class MapArray(
  vector: MapVector,
  elements: StructArray
) extends ListArray[MapVector, ArrowRecord](
  scalaType = ru.typeOf[Iterable[ArrowRecord]],
  vector = vector,
  elements = elements
) {
  def keyArray: ArrowArray.Typed[_, _] = elements.childAt(0)

  def valueArray: ArrowArray.Typed[_, _] = elements.childAt(1)

  override def innerAs(tpe: ru.Type): ArrowArray.Typed[MapVector, _] = {
    if (!ReflectUtils.isMap(tpe)) {
      // Fallback to ListArray behavior
      super.innerAs(tpe)
    }

    val (keyType, valueType) = (ReflectUtils.typeArgument(tpe, 0), ReflectUtils.typeArgument(tpe, 1))

    val castedKeys = keyArray.as(keyType)
    val castedValues = valueArray.as(valueType)

    (castedKeys, castedValues) match {
      case (k: ArrowArray.Typed[_, k2], v: ArrowArray.Typed[_, v2]) =>
        // Check the collection type conversions
        val targetCollectionType = tpe.typeConstructor
        val castedStruct = elements.copy(children = Seq(k, v))

        if (targetCollectionType =:= ru.typeOf[collection.immutable.Map[_, _]].typeConstructor) {
          val getter = (
            arr: LogicalArray[MapArray, MapVector, ArrowArray.Typed[_, ArrowRecord], collection.immutable.Map[k2, v2]],
            index: Int
          ) => {
            val keyValues = arr.childAt(0).asInstanceOf[StructArray]
            val (start, end) = arr.inner.getStartEnd(index)
            val kv = keyValues.unsafeGetTuples[k2, v2](start, end)
            collection.immutable.Map(kv: _*)
          }

          val setter = (
            arr: LogicalArray[MapArray, MapVector, ArrowArray.Typed[_, ArrowRecord], Map[k2, v2]],
            index: Int, value: Map[k2, v2]
          ) => {
            arr.inner.vector.startNewValue(index)

            val keyValues = arr.childAt(0).asInstanceOf[StructArray]
            val start = arr.inner.getStartEnd(index)._1
            keyValues.unsafeSetTuples[k2, v2](start, value)

            arr.inner.vector.endValue(index, value.size)
            ()
          }

          new LogicalArray[MapArray, MapVector, ArrowArray.Typed[_, ArrowRecord], Map[k2, v2]](
            scalaType = tpe,
            inner = this,
            getter = getter,
            setter = setter,
            children = Seq(castedStruct)
          )
        }
        else {
          throw new IllegalArgumentException(s"Unsupported target collection type: $targetCollectionType. Only immutable.Map is supported.")
        }
    }
  }
}

object MapArray {
  def default(vector: MapVector): MapArray = {
    val elements = ArrowArray
      .from(vector.getDataVector)
      .asInstanceOf[StructArray]

    new MapArray(
      vector = vector,
      elements = elements
    )
  }
}

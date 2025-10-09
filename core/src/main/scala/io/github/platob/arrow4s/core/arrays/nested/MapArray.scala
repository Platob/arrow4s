package io.github.platob.arrow4s.core.arrays.nested

import io.github.platob.arrow4s.core.arrays.{ArrowArray, LogicalArray}
import io.github.platob.arrow4s.core.codec.ValueCodec
import io.github.platob.arrow4s.core.reflection.ReflectUtils
import org.apache.arrow.vector.complex.{MapVector, StructVector}

import scala.reflect.runtime.{universe => ru}

class MapArray[K, V](
  vector: MapVector,
  elements: StructArray[(K, V)]
) extends ListArray.Base[
  (K, V), ArrowArray[(K, V)], MapVector, MapArray[K, V]
](vector = vector, elements = elements) {
  def keyArray: ArrowArray[K] = elements.childAt(0).asInstanceOf[ArrowArray[K]]

  def valueArray: ArrowArray[V] = elements.childAt(1).asInstanceOf[ArrowArray[V]]

  override def get(index: Int): ArrowArray[(K, V)] = {
    val (start, end) = getStartEnd(index)

    elements.arrowSlice(start = start, end = end)
  }

  override def set(index: Int, value: ArrowArray[(K, V)]): this.type = {
    val start = getStartEnd(index)._1

    this.startNewValue(index)

    var i = 0
    value.foreach { v =>
      val (key, value) = v
      keyArray.set(start + i, key)
      valueArray.set(start + i, value)
      i += 1
    }

    this.endValue(index, i)

    this
  }

  override def innerAs(codec: ValueCodec[_]): ArrowArray[_] = {
    if (!ReflectUtils.isMap(codec.tpe)) {
      // Fallback to ListArray behavior
      super.innerAs(codec)
    }

    val keyValue = codec.childAt(0)
    val (keyType, valueType) = (keyValue.childAt(0), keyValue.childAt(1))

    val castedKeys = keyArray.asUnsafe(keyType)
    val castedValues = valueArray.asUnsafe(valueType)

    (castedKeys, castedValues) match {
      case (k: ArrowArray.Typed[k2, _, _], v: ArrowArray.Typed[v2, _, _]) =>
        // Check the collection type conversions
        val pairs = StructArray.tuple2[k2, v2](elements.vector, k, v)
        val targetCollectionType = codec.tpe.typeConstructor

        if (targetCollectionType =:= ru.typeOf[collection.immutable.Map[_, _]].typeConstructor) {
          type LM = LogicalArray.Map[K, V, k2, v2, collection.immutable.Map[k2, v2]]

          val getter = (arr: LM, index: Int) => {
            val tp = arr.getTuples(index)

            tp.toMap
          }

          val setter = (arr: LM, index: Int, value: collection.immutable.Map[k2, v2]) => {
            arr.setTuples(index = index, value)
          }

          LogicalArray.map[K, V, k2, v2, collection.immutable.Map[k2, v2]](
            inner = this,
            codec = codec.asInstanceOf[ValueCodec[collection.immutable.Map[k2, v2]]],
            getter = getter,
            setter = setter,
            pairs = pairs
          )
        }
        else {
          throw new IllegalArgumentException(s"Unsupported target collection type: $targetCollectionType. Only immutable.Map is supported.")
        }
    }
  }
}

object MapArray {
  def default(vector: MapVector): MapArray[_, _] = {
    val elements = StructArray.tuple2(vector.getDataVector.asInstanceOf[StructVector])
    new MapArray(vector = vector, elements)
  }
}

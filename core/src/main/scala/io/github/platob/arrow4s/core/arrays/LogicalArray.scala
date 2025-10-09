package io.github.platob.arrow4s.core
package arrays

import io.github.platob.arrow4s.core.arrays.nested.{ListArray, MapArray, StructArray}
import io.github.platob.arrow4s.core.arrays.primitive.PrimitiveArray
import io.github.platob.arrow4s.core.codec.ValueCodec
import io.github.platob.arrow4s.core.values.ValueConverter
import org.apache.arrow.vector.complex.{ListVector, MapVector, StructVector}
import org.apache.arrow.vector.{FieldVector, ValueVector}

trait LogicalArray[
  ArrowVector <: ValueVector,
  Inner, InArray <: ArrowArray.Typed[Inner, ArrowVector, InArray],
  Outer, Arr <: LogicalArray[ArrowVector, Inner, InArray, Outer, Arr]
] extends ArrowArrayProxy.Typed[ArrowVector, Inner, InArray, Outer, Arr] {
  override def isLogical: Boolean = true
}

object LogicalArray {
  class Optional[Inner, ArrowVector <: ValueVector, InArray <: ArrowArray.Typed[Inner, ArrowVector, InArray]](
    val inner: InArray,
    val codec: ValueCodec[Option[Inner]],
  ) extends LogicalArray[
    ArrowVector, Inner, InArray, Option[Inner], Optional[Inner, ArrowVector, InArray]
  ] {
    override def isOptional: Boolean = true

    override def children: Seq[ArrowArray[_]] = inner.children

    // Accessors
    override def get(index: Int): Option[Inner] = {
      if (inner.isNull(index)) {
        None
      } else {
        Some(inner.get(index))
      }
    }

    // Mutators
    override def set(index: Int, value: Option[Inner]): this.type = {
      value match {
        case Some(v) =>
          inner.set(index, v)
        case None =>
          inner.setNull(index)
      }
      this
    }
  }

  class Converter[ArrowVector <: ValueVector, Inner, InArray <: ArrowArray.Typed[Inner, ArrowVector, InArray], Outer](
    val inner: InArray,
    val codec: ValueCodec[Outer],
    val converter: ValueConverter[Inner, Outer],
  ) extends LogicalArray[ArrowVector, Inner, InArray, Outer, Converter[ArrowVector, Inner, InArray, Outer]] {
    override def children: Seq[ArrowArray.Typed[_, _, _]] = {
      throw new UnsupportedOperationException("LogicalArray.Converter does not support children.")
    }

    // Accessors
    override def get(index: Int): Outer = {
      converter.decode(inner.get(index))
    }

    // Mutators
    override def set(index: Int, value: Outer): this.type = {
      val converted = converter.encode(value)
      inner.set(index, converted)
      this
    }
  }

  class List[In, Out, Outer](
    val inner: ListArray[In],
    val codec: ValueCodec[Outer],
    val getter: (List[In, Out, Outer], Int) => Outer,
    val setter: (List[In, Out, Outer], Int, Outer) => Unit,
    val elements: ArrowArray.Typed[Out, _, _],
  ) extends LogicalArray[ListVector, ArrowArray[In], ListArray[In], Outer, List[In, Out, Outer]] {
    override def children: Seq[ArrowArray.Typed[Out, _, _]] = Seq(elements)

    def getStartEnd(index: Int): (Int, Int) = inner.getStartEnd(index)

    override def get(index: Int): Outer = {
      getter(this, index)
    }

    def getElements(index: Int): IndexedSeq[Out] = {
      val (start, end) = getStartEnd(index)

      val result = (start until end).map(elements.get)

      result
    }

    override def set(index: Int, value: Outer): this.type = {
      setter(this, index, value)
      this
    }

    def setElements(index: Int, values: Iterable[Out]): Unit = {
      inner.startNewValue(index)
      // Get the start index for the new list entry
      val start = inner.getStartEnd(index)._1

      elements.setValues(index = start, values)

      inner.endValue(index, values.size)
    }
  }

  class Map[InKey, InValue, OutKey, OutValue, Outer](
    val inner: MapArray[InKey, InValue],
    val codec: ValueCodec[Outer],
    val getter: (Map[InKey, InValue, OutKey, OutValue, Outer], Int) => Outer,
    val setter: (Map[InKey, InValue, OutKey, OutValue, Outer], Int, Outer) => Unit,
    val pairs: StructArray[(OutKey, OutValue)],
  ) extends LogicalArray[
    MapVector, ArrowArray[(InKey, InValue)], MapArray[InKey, InValue],
    Outer, Map[InKey, InValue, OutKey, OutValue, Outer]
  ] {
    override def children: Seq[StructArray[(OutKey, OutValue)]] = Seq(pairs)

    def getStartEnd(index: Int): (Int, Int) = inner.getStartEnd(index)

    override def get(index: Int): Outer = {
      getter(this, index)
    }

    def getTuples(index: Int): IndexedSeq[(OutKey, OutValue)] = {
      val (start, end) = getStartEnd(index)

      val result = (start until end).map(pairs.get)

      result
    }

    override def set(index: Int, value: Outer): this.type = {
      setter(this, index, value)
      this
    }

    def setTuples(index: Int, values: Iterable[(OutKey, OutValue)]): Unit = {
      inner.startNewValue(index)
      // Get the start index for the new map entry
      val start = inner.getStartEnd(index)._1

      var i = 0
      for ((k, v) <- values) {
        val itemIndex = start + i

        pairs.set(index = itemIndex, value = (k, v))

        i += 1
      }

      inner.endValue(index, values.size)
    }
  }

  class Struct[Inner, Outer](
    val inner: StructArray[Inner],
    val codec: ValueCodec[Outer],
    val children: Seq[ArrowArray[_]],
  ) extends LogicalArray[StructVector, Inner, StructArray[Inner], Outer, Struct[Inner, Outer]] {
    override def get(index: Int): Outer = {
      val values = children.map(_.get(index)).toArray

      codec.fromElements(values)
    }

    override def set(index: Int, value: Outer): this.type = {
      val elements = codec.elements(value)

      elements.zipWithIndex.foreach { case (v, i) =>
        val child = children(i)
        child.unsafeSet(index, v)
      }

      inner.setIndexDefined(index) // mark the struct itself non-null

      this
    }
  }

  def optional[
    Value, ArrowVector <: ValueVector, Arr <: ArrowArray.Typed[Value, ArrowVector, Arr]
  ](arr: Arr, codec: ValueCodec[Option[Value]]): Optional[Value, ArrowVector, Arr] = {
    new Optional[Value, ArrowVector, Arr](arr, codec)
  }

  def convertPrimitive[
    Value, ArrowVector <: FieldVector, Arr <: PrimitiveArray.Typed[Value, ArrowVector, Arr]
  ](arr: Arr, codec: ValueCodec[_]): Converter[ArrowVector, Value, Arr, _] = {
    val converter = ValueConverter.create(from = arr.codec, to = codec)

    converter match {
      case vc: ValueConverter[v, t] @unchecked =>
        val asIs = vc.asInstanceOf[ValueConverter[Value, t]]

        LogicalArray.converter[Value, ArrowVector, Arr, t](
          array = arr,
          codec = codec.asInstanceOf[ValueCodec[t]],
        )(converter = asIs)
    }
  }

  def converter[Inner, ArrowVector <: ValueVector, InArray <: ArrowArray.Typed[Inner, ArrowVector, InArray], Outer](
    array: InArray,
    codec: ValueCodec[Outer],
  )(implicit converter: ValueConverter[Inner, Outer]): Converter[ArrowVector, Inner, InArray, Outer] = {
    new Converter[ArrowVector, Inner, InArray, Outer](
      inner = array,
      codec = codec,
      converter = converter
    )
  }

  def list[In, Out, Outer](
    inner: ListArray[In],
    codec: ValueCodec[Outer],
    getter: (List[In, Out, Outer], Int) => Outer,
    setter: (List[In, Out, Outer], Int, Outer) => Unit,
    elements: ArrowArray.Typed[Out, _, _],
  ): List[In, Out, Outer] = {
    new List[In, Out, Outer](
      inner = inner,
      codec = codec,
      getter = getter,
      setter = setter,
      elements = elements
    )
  }

  def map[InKey, InValue, OutKey, OutValue, Outer](
    inner: MapArray[InKey, InValue],
    codec: ValueCodec[Outer],
    getter: (Map[InKey, InValue, OutKey, OutValue, Outer], Int) => Outer,
    setter: (Map[InKey, InValue, OutKey, OutValue, Outer], Int, Outer) => Unit,
    pairs: StructArray[(OutKey, OutValue)],
  ): Map[InKey, InValue, OutKey, OutValue, Outer] = {
    new Map[InKey, InValue, OutKey, OutValue, Outer](
      inner = inner,
      codec = codec,
      getter = getter,
      setter = setter,
      pairs = pairs
    )
  }

  def struct[Inner, Outer](
    array: StructArray[Inner],
    codec: ValueCodec[Outer],
    children: Seq[ArrowArray[_]],
  ): Struct[Inner, Outer] = {
    new Struct[Inner, Outer](
      inner = array,
      codec = codec,
      children = children
    )
  }
}
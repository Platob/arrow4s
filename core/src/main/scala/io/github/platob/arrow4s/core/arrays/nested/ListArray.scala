package io.github.platob.arrow4s.core.arrays.nested

import io.github.platob.arrow4s.core.arrays.{ArraySlice, ArrowArray}
import io.github.platob.arrow4s.core.arrays.traits.TArrowArray
import io.github.platob.arrow4s.core.codec.ValueCodec
import io.github.platob.arrow4s.core.codec.nested.ListCodec
import org.apache.arrow.vector.complex.ListVector
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID

class ListArray[Item](
  vector: ListVector,
  elements: ArrowArray.Typed[Item, _, _]
) extends ListArray.Base[Item, ArrowArray[Item], ListVector, ListArray[Item]](vector, elements) {
  override def get(index: Int): ArraySlice.Typed[Item, _, _] = {
    val (start, end) = getStartEnd(index)

    elements.arrowSlice(start = start, end = end)
  }

  override def set(index: Int, value: ArrowArray[Item]): this.type = {
    val start = getStartEnd(index)._1

    this.startNewValue(index)

    var i = 0
    value.foreach(v => {
      elements.set(start + i, v)
      i += 1
    })

    this.endValue(index, i)

    this
  }
}

object ListArray {
  trait TListArray extends TArrowArray {

  }

  abstract class Base[
    Item,
    Value <: ArrowArray[Item],
    ArrowVector <: ListVector,
    Arr <: Base[Item, Value, ArrowVector, Arr]
  ](vector: ArrowVector, val elements: ArrowArray.Typed[Item, _, _])
    extends NestedArray.Typed[Value, ArrowVector, Arr](
      vector = vector,
      codec = ValueCodec.fromField(arrowField = vector.getField).asInstanceOf[ValueCodec[Value]]
    ) with TListArray {
    override def children: Seq[ArrowArray.Typed[Item, _, _]] = Seq(elements)

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

    def startNewValue(index: Int): this.type = {
      vector.startNewValue(index)
      this
    }

    def endValue(index: Int, count: Int): this.type = {
      vector.endValue(index, count)
      this
    }

    override def setNull(index: Int): this.type = {
      this.vector.setNull(index)
      this
    }

    override def innerAs(codec: ValueCodec[_]): ArrowArray[_] = {
      codec.arrowTypeId match {
        case ArrowTypeID.List =>
          // Cast the elements to the target type
          val castedElements = elements.asUnsafe(codec.childAt(0))

          castedElements match {
            case e: ArrowArray.Typed[t, _, _] @unchecked =>
              val listCodec = codec.asInstanceOf[ListCodec[t, _]]
              val base = new ListArray[t](
                vector = this.vector,
                elements = e
              )

              listCodec.arrayAsCollection(base = base)
            case _ =>
              throw new IllegalStateException(s"Expected elements to be of type ArrowArray.Typed, but got: ${castedElements.getClass}")
          }
        case _ =>
          elements.asUnsafe(codec)
      }
    }
  }

  def default(vector: ListVector): ListArray[_] = {
    val elements = ArrowArray.from(vector.getDataVector)

    elements match {
      case e: ArrowArray.Typed[t, _, _] @unchecked =>
        new ListArray[t](vector = vector, elements = e)
    }
  }
}
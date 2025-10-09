package io.github.platob.arrow4s.core.codec.nested

import io.github.platob.arrow4s.core.arrays.{ArrowArray, LogicalArray}
import io.github.platob.arrow4s.core.arrays.nested.ListArray
import io.github.platob.arrow4s.core.codec.ValueCodec
import io.github.platob.arrow4s.core.types.ArrowField
import org.apache.arrow.vector.types.pojo.{ArrowType, Field}

import scala.reflect.runtime.{universe => ru}
import scala.reflect.{ClassTag, classTag}

class ListCodec[Item, CC[X]](
  val elementCodec: ValueCodec[Item],
  val toCollection: IndexedSeq[Item] => CC[Item],
  val fromCollection: CC[Item] => IndexedSeq[Item],
  tpe: ru.Type,
  typeTag: ru.TypeTag[CC[Item]],
  clsTag: ClassTag[CC[Item]]
) extends NestedCodec[CC[Item]](tpe = tpe, typeTag = typeTag, clsTag = clsTag) {
  override val namespace: String = tpe.typeSymbol.fullName

  override val arrowField: Field = {
    val child = elementCodec.arrowField

    ArrowField.build(
      name = this.namespace,
      at = ArrowType.List.INSTANCE,
      nullable = false,
      children = Seq(child),
      metadata = None
    )
  }

  override def elementAt[Elem](value: CC[Item], index: Int): Elem = {
    fromCollection(value)(index).asInstanceOf[Elem]
  }

  override def fromElements(values: Array[Any]): CC[Item] = {
    toCollection(values.toIndexedSeq.map(_.asInstanceOf[Item]))
  }

  override def children: Seq[ValueCodec[Item]] = Seq(elementCodec)

  def arrayAsCollection(base: ListArray[Item]): LogicalArray.List[Item, Item, CC[Item]] = {
    type LM = LogicalArray.List[Item, Item, CC[Item]]

    val getter = (arr: LM, index: Int) => {
      val tp = arr.getElements(index)

      toCollection(tp)
    }

    val setter = (arr: LM, index: Int, value: CC[Item]) => {
      val seq = fromCollection(value)

      arr.setElements(index, seq)
    }

    LogicalArray.list[Item, Item, CC[Item]](
      inner = base,
      codec = this,
      getter = getter,
      setter = setter,
      elements = base.elements
    )
  }
}

object ListCodec {
  def reflect(item: ValueCodec[_]): ListCodec[_, List] = {
    item match {
      case vc: ValueCodec[t] =>
        list[t](vc)
    }
  }

  def arrowArray[ITEM](implicit elementCodec: ValueCodec[ITEM]): ListCodec[ITEM, ArrowArray] = {
    implicit val itt: ru.TypeTag[ITEM] = elementCodec.typeTag
    implicit val ict: ClassTag[ITEM] = elementCodec.clsTag

    val tt = ru.typeTag[ArrowArray[ITEM]]
    val ct = classTag[ArrowArray[ITEM]]

    new ListCodec(
      elementCodec = elementCodec,
      toCollection = (seq: IndexedSeq[ITEM]) => ArrowArray.make[ITEM](seq)(elementCodec),
      fromCollection = (arr: ArrowArray[ITEM]) => arr,
      tpe = tt.tpe,
      typeTag = tt,
      clsTag = ct
    )
  }

  def seq[ITEM](implicit elementCodec: ValueCodec[ITEM]): ListCodec[ITEM, Seq] = {
    implicit val itt: ru.TypeTag[ITEM] = elementCodec.typeTag
    implicit val ict: ClassTag[ITEM] = elementCodec.clsTag

    val tt = ru.typeTag[Seq[ITEM]]
    val ct = classTag[Seq[ITEM]]

    new ListCodec(
      elementCodec = elementCodec,
      toCollection = (seq: IndexedSeq[ITEM]) => seq,
      fromCollection = (arr: Seq[ITEM]) => arr.toIndexedSeq,
      tpe = tt.tpe,
      typeTag = tt,
      clsTag = ct
    )
  }

  def list[ITEM](implicit elementCodec: ValueCodec[ITEM]): ListCodec[ITEM, List] = {
    implicit val itt: ru.TypeTag[ITEM] = elementCodec.typeTag
    implicit val ict: ClassTag[ITEM] = elementCodec.clsTag

    val tt = ru.typeTag[List[ITEM]]
    val ct = classTag[List[ITEM]]

    new ListCodec(
      elementCodec = elementCodec,
      toCollection = (seq: IndexedSeq[ITEM]) => seq.toList,
      fromCollection = (arr: List[ITEM]) => arr.toIndexedSeq,
      tpe = tt.tpe,
      typeTag = tt,
      clsTag = ct
    )
  }
}

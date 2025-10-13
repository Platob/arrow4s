package io.github.platob.arrow4s.core.codec.value.nested

import io.github.platob.arrow4s.core.arrays.ArrowArray
import io.github.platob.arrow4s.core.codec.value.ValueCodec
import io.github.platob.arrow4s.core.types.ArrowField
import org.apache.arrow.vector.types.pojo.{ArrowType, Field}

import scala.reflect.runtime.{universe => ru}
import scala.reflect.{ClassTag, classTag}

class ListValueCodec[Item, CC[X]](
  val elementCodec: ValueCodec[Item],
  val toCollection: IndexedSeq[Item] => CC[Item],
  val fromCollection: CC[Item] => IndexedSeq[Item],
  tpe: ru.Type,
  typeTag: ru.TypeTag[CC[Item]],
  clsTag: ClassTag[CC[Item]]
) extends NestedValueCodec[CC[Item]](tpe = tpe, typeTag = typeTag, clsTag = clsTag) {
  override val namespace: String = tpe.typeSymbol.fullName

  override def isTuple: Boolean = false

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
}

object ListValueCodec {
  def reflect(item: ValueCodec[_]): ListValueCodec[_, List] = {
    item match {
      case vc: ValueCodec[t] =>
        list[t](vc)
    }
  }

  def arrowArray[ITEM](implicit elementCodec: ValueCodec[ITEM]): ListValueCodec[ITEM, ArrowArray] = {
    implicit val itt: ru.TypeTag[ITEM] = elementCodec.typeTag
    implicit val ict: ClassTag[ITEM] = elementCodec.clsTag

    val tt = ru.typeTag[ArrowArray[ITEM]]
    val ct = classTag[ArrowArray[ITEM]]

    new ListValueCodec(
      elementCodec = elementCodec,
      toCollection = (seq: IndexedSeq[ITEM]) => ArrowArray.make[ITEM](seq)(elementCodec),
      fromCollection = (arr: ArrowArray[ITEM]) => arr,
      tpe = tt.tpe,
      typeTag = tt,
      clsTag = ct
    )
  }

  def seq[ITEM](implicit elementCodec: ValueCodec[ITEM]): ListValueCodec[ITEM, Seq] = {
    implicit val itt: ru.TypeTag[ITEM] = elementCodec.typeTag
    implicit val ict: ClassTag[ITEM] = elementCodec.clsTag

    val tt = ru.typeTag[Seq[ITEM]]
    val ct = classTag[Seq[ITEM]]

    new ListValueCodec(
      elementCodec = elementCodec,
      toCollection = (seq: IndexedSeq[ITEM]) => seq,
      fromCollection = (arr: Seq[ITEM]) => arr.toIndexedSeq,
      tpe = tt.tpe,
      typeTag = tt,
      clsTag = ct
    )
  }

  def list[ITEM](implicit elementCodec: ValueCodec[ITEM]): ListValueCodec[ITEM, List] = {
    implicit val itt: ru.TypeTag[ITEM] = elementCodec.typeTag
    implicit val ict: ClassTag[ITEM] = elementCodec.clsTag

    val tt = ru.typeTag[List[ITEM]]
    val ct = classTag[List[ITEM]]

    new ListValueCodec(
      elementCodec = elementCodec,
      toCollection = (seq: IndexedSeq[ITEM]) => seq.toList,
      fromCollection = (arr: List[ITEM]) => arr.toIndexedSeq,
      tpe = tt.tpe,
      typeTag = tt,
      clsTag = ct
    )
  }
}

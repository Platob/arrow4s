package io.github.platob.arrow4s.core.codec.nested

import io.github.platob.arrow4s.core.codec.ValueCodec
import io.github.platob.arrow4s.core.types.ArrowField
import org.apache.arrow.vector.types.pojo.Field

import scala.reflect.{ClassTag, classTag}
import scala.reflect.runtime.{universe => ru}

class Tuple2Codec[T1, T2](
  val parent: Field,
  val t1: ValueCodec[T1],
  val t2: ValueCodec[T2],
  tpe: ru.Type,
  typeTag: ru.TypeTag[(T1, T2)],
  clsTag: ClassTag[(T1, T2)]
) extends StructCodec[(T1, T2)](
  arrowField = parent,
  children = Seq(t1, t2),
  tpe = tpe,
  typeTag = typeTag,
  clsTag = clsTag
) {
  override def elementAt[Elem](value: (T1, T2), index: Int): Elem = {
    index match {
      case 0 => value._1.asInstanceOf[Elem]
      case 1 => value._2.asInstanceOf[Elem]
      case _ => throw new IndexOutOfBoundsException(s"Tuple2 has only 2 elements, got index $index")
    }
  }

  override def fromElements(values: Array[Any]): (T1, T2) = {
    (values(0).asInstanceOf[T1], values(1).asInstanceOf[T2])
  }
}

object Tuple2Codec {
  def instance[T1, T2](
    parent: Field,
    t1: ValueCodec[T1],
    t2: ValueCodec[T2]
  )(implicit tt: ru.TypeTag[(T1, T2)], ct: ClassTag[(T1, T2)]): Tuple2Codec[T1, T2] = {
    new Tuple2Codec[T1, T2](
      parent = parent,
      t1 = t1,
      t2 = t2,
      tpe = tt.tpe,
      typeTag = tt,
      clsTag = ct
    )
  }

  implicit def fromCodecs[T1, T2](implicit t1: ValueCodec[T1], t2: ValueCodec[T2]): Tuple2Codec[T1, T2] = {
    fromCodecs(t1, t2, None)
  }

  def fromCodecs[T1, T2](
    t1: ValueCodec[T1], t2: ValueCodec[T2],
    arrowField: Option[Field],
  ): Tuple2Codec[T1, T2] = {
    implicit val tt1: ru.TypeTag[T1] = t1.typeTag
    implicit val ct1: ClassTag[T1] = t1.clsTag

    implicit val tt2: ru.TypeTag[T2] = t2.typeTag
    implicit val ct2: ClassTag[T2] = t2.clsTag

    implicit val tt = ru.typeTag[(T1, T2)]
    implicit val ct = classTag[(T1, T2)]
    val tpe = tt.tpe

    val parent = arrowField.getOrElse(ArrowField.struct(
      name = tpe.typeSymbol.fullName,
      children = Seq(t1.arrowField, t2.arrowField),
      nullable = false,
      metadata = Some(Map("namespace" -> tpe.typeSymbol.fullName))
    ))

    instance(parent, t1, t2)
  }

  def fromFields(arrowField: Field, f1: Field, f2: Field): Tuple2Codec[_, _] = {
    val c1 = ValueCodec.fromField(f1)
    val c2 = ValueCodec.fromField(f2)

    (c1, c2) match {
      case (vc1: ValueCodec[t1], vc2: ValueCodec[t2]) =>
        fromCodecs(vc1, vc2, Some(arrowField))
      case _ =>
        throw new NotImplementedError("Tuple2Codec.fromFields is only implemented for concrete ValueCodec types")
    }
  }
}
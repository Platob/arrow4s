package io.github.platob.arrow4s.core.codec.nested

import io.github.platob.arrow4s.core.codec.ValueCodec
import io.github.platob.arrow4s.core.types.ArrowField
import org.apache.arrow.vector.types.pojo.Field

import scala.reflect.runtime.{universe => ru}
import scala.reflect.{ClassTag, classTag}

class Tuple3Codec[T1, T2, T3](
  val parent: Field,
  val t1: ValueCodec[T1],
  val t2: ValueCodec[T2],
  val t3: ValueCodec[T3],
  tpe: ru.Type,
  typeTag: ru.TypeTag[(T1, T2, T3)],
  clsTag: ClassTag[(T1, T2, T3)]
) extends StructCodec[(T1, T2, T3)](
  arrowField = parent,
  children = Seq(t1, t2, t3),
  tpe = tpe,
  typeTag = typeTag,
  clsTag = clsTag
) {
  override def elementAt[Elem](value: (T1, T2, T3), index: Int): Elem = {
    index match {
      case 0 => value._1.asInstanceOf[Elem]
      case 1 => value._2.asInstanceOf[Elem]
      case 2 => value._3.asInstanceOf[Elem]
      case _ => throw new IndexOutOfBoundsException(s"Tuple2 has only 2 elements, got index $index")
    }
  }

  override def fromElements(values: Array[Any]): (T1, T2, T3) = {
    (
      values(0).asInstanceOf[T1],
      values(1).asInstanceOf[T2],
      values(2).asInstanceOf[T3]
    )
  }
}

object Tuple3Codec {
  def instance[T1, T2, T3](
    parent: Field,
    t1: ValueCodec[T1],
    t2: ValueCodec[T2],
    t3: ValueCodec[T3],
  )(implicit tt: ru.TypeTag[(T1, T2, T3)], ct: ClassTag[(T1, T2, T3)]): Tuple3Codec[T1, T2, T3] = {
    new Tuple3Codec[T1, T2, T3](
      parent = parent,
      t1 = t1,
      t2 = t2,
      t3 = t3,
      tpe = tt.tpe,
      typeTag = tt,
      clsTag = ct
    )
  }

  implicit def fromCodecs[T1, T2, T3](implicit
    t1: ValueCodec[T1],
    t2: ValueCodec[T2],
    t3: ValueCodec[T3]
  ): Tuple3Codec[T1, T2, T3] = {
    fromCodecs(t1, t2, t3, None)
  }

  def fromCodecs[T1, T2, T3](
    t1: ValueCodec[T1], t2: ValueCodec[T2], t3: ValueCodec[T3],
    arrowField: Option[Field],
  ): Tuple3Codec[T1, T2, T3] = {
    implicit val tt1: ru.TypeTag[T1] = t1.typeTag
    implicit val ct1: ClassTag[T1] = t1.clsTag

    implicit val tt2: ru.TypeTag[T2] = t2.typeTag
    implicit val ct2: ClassTag[T2] = t2.clsTag

    implicit val tt3: ru.TypeTag[T3] = t3.typeTag
    implicit val ct3: ClassTag[T3] = t3.clsTag

    implicit val tt = ru.typeTag[(T1, T2, T3)]
    implicit val ct = classTag[(T1, T2, T3)]
    val tpe = tt.tpe

    val parent = arrowField.getOrElse(ArrowField.struct(
      name = tpe.typeSymbol.fullName,
      children = Seq(t1.arrowField, t2.arrowField, t3.arrowField),
      nullable = false,
      metadata = Some(Map("namespace" -> tpe.typeSymbol.fullName))
    ))

    instance(parent, t1, t2, t3)(tt, ct)
  }

  def fromFields(arrowField: Field, f1: Field, f2: Field, f3: Field): Tuple3Codec[_, _, _] = {
    val c1 = ValueCodec.fromField(f1)
    val c2 = ValueCodec.fromField(f2)
    val c3 = ValueCodec.fromField(f3)

    (c1, c2, c3) match {
      case (vc1: ValueCodec[t1], vc2: ValueCodec[t2], vc3: ValueCodec[t3]) =>
        fromCodecs[t1, t2, t3](vc1, vc2, vc3, Some(arrowField))
      case _ =>
        throw new NotImplementedError("Tuple3Codec.fromFields is only implemented for concrete ValueCodec types")
    }
  }
}
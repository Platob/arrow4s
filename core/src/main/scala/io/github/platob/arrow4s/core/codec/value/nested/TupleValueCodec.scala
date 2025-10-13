package io.github.platob.arrow4s.core.codec.value.nested

import io.github.platob.arrow4s.core.codec.value.ValueCodec
import io.github.platob.arrow4s.core.types.ArrowField
import org.apache.arrow.vector.types.pojo.Field

import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.reflect.{ClassTag, classTag}
import scala.reflect.runtime.{universe => ru}

abstract class TupleValueCodec[P <: Product](
  arrowField: Field,
  children: Seq[ValueCodec[_]],
  tpe: ru.Type,
  typeTag: ru.TypeTag[P],
  clsTag: ClassTag[P]
) extends StructValueCodec[P](
  arrowField = arrowField,
  children = children,
  tpe = tpe,
  typeTag = typeTag,
  clsTag = clsTag
) {
  override def isTuple: Boolean = true
}

object TupleValueCodec {
  class Tuple2ValueCodec[T1, T2](
    val t1: ValueCodec[T1],
    val t2: ValueCodec[T2],
    arrowField: Field,
    tpe: ru.Type,
    typeTag: ru.TypeTag[(T1, T2)],
    clsTag: ClassTag[(T1, T2)]
  ) extends TupleValueCodec[(T1, T2)](
      arrowField = arrowField,
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

  def makeArrowField(children: Seq[ValueCodec[_]], tpe: ru.Type): Field = {
    ArrowField.struct(
      name = tpe.typeSymbol.fullName,
      children = children.zipWithIndex.map {
        case (c, i) =>
          ArrowField.rename(c.arrowField, s"_${i + 1}")
      },
      nullable = false,
      metadata = Some(Map("namespace" -> tpe.typeSymbol.fullName))
    )
  }

  def tupleFromField(field: Field): TupleValueCodec[_] = {
    val as = field.getChildren.asScala.map(ValueCodec.fromField).toSeq

    tuple(as)
  }

  def tuple(children: Seq[ValueCodec[_]]): TupleValueCodec[_] = {
    children.size match {
      case 2 =>
        val f1 = children.head
        val f2 = children(1)

        (f1, f2) match {
          case (c1: ValueCodec[t1], c2: ValueCodec[t2]) =>
            implicit val tt1 = c1.typeTag
            implicit val tt2 = c2.typeTag

            implicit val ct1 = c1.clsTag
            implicit val ct2 = c2.clsTag
            tuple2[t1, t2](c1, c2, ru.typeTag[(t1, t2)], classTag[(t1, t2)])
        }
      case _ =>
        throw new NotImplementedError("StructCodec.tuple is only implemented for Tuple2 and Tuple3")
    }
  }

  implicit def tuple2[T1, T2](implicit
    t1: ValueCodec[T1], t2: ValueCodec[T2],
    tt: ru.TypeTag[(T1, T2)], ct: ClassTag[(T1, T2)],
  ): Tuple2ValueCodec[T1, T2] = {
    val arrowField = makeArrowField(Seq(t1, t2), tt.tpe)

    new Tuple2ValueCodec[T1, T2](
      t1 = t1,
      t2 = t2,
      arrowField = arrowField,
      tpe = tt.tpe,
      typeTag = tt,
      clsTag = ct
    )
  }
}
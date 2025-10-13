package io.github.platob.arrow4s.core.codec.value.nested

import TupleValueCodec.Tuple2ValueCodec
import io.github.platob.arrow4s.core.types.ArrowField
import org.apache.arrow.vector.types.pojo.{ArrowType, Field}

import scala.reflect.runtime.{universe => ru}
import scala.reflect.{ClassTag, classTag}

class MapValueCodec[KEY, VALUE, CC[X, Y] <: scala.collection.Map[X, Y]](
  val pair: Tuple2ValueCodec[KEY, VALUE],
  tpe: ru.Type,
  typeTag: ru.TypeTag[CC[KEY, VALUE]],
  clsTag: ClassTag[CC[KEY, VALUE]]
) extends NestedValueCodec[CC[KEY, VALUE]](tpe = tpe, typeTag = typeTag, clsTag = clsTag) {
  override val children: Seq[Tuple2ValueCodec[KEY, VALUE]] = Seq(pair)

  override def isTuple: Boolean = false

  override def arrowField: Field = {
    val fields = children.map(_.arrowField)

    ArrowField.build(
      name = namespace,
      at = new ArrowType.Map(false),
      nullable = false,
      children = fields,
      metadata = Some(Map("namespace" -> namespace))
    )
  }

  override def elementAt[Elem](value: CC[KEY, VALUE], index: Int): Elem = {
    // Get the tuple at the index
    value.toSeq.lift(index) match {
      case Some((k, v)) => (k, v).asInstanceOf[Elem]
      case None         => throw new IndexOutOfBoundsException(s"Map has no element at index $index")
    }
  }

  override def fromElements(values: Array[Any]): CC[KEY, VALUE] = {
    values.map(_.asInstanceOf[(KEY, VALUE)]).toMap.asInstanceOf[CC[KEY, VALUE]]
  }
}

object MapValueCodec {
  def map[K, V](implicit
    pair: Tuple2ValueCodec[K, V],
    tt: ru.TypeTag[Map[K, V]],
    ct: ClassTag[Map[K, V]]
  ): MapValueCodec[K, V, Map] = {
    new MapValueCodec[K, V, Map](
      pair = pair,
      tpe = tt.tpe,
      typeTag = tt,
      clsTag = ct
    )
  }

  def mapFromPair[K, V](pair: Tuple2ValueCodec[K, V]): MapValueCodec[K, V, Map] = {
    implicit val tt1: ru.TypeTag[K] = pair.t1.typeTag
    implicit val ct1: ClassTag[K] = pair.t1.clsTag

    implicit val tt2: ru.TypeTag[V] = pair.t2.typeTag
    implicit val ct2: ClassTag[V] = pair.t2.clsTag

    val tt = ru.typeTag[Map[K, V]]
    val ct = classTag[Map[K, V]]

    map[K, V](pair, tt, ct)
  }

  def reflect(pair: Tuple2ValueCodec[_, _]): MapValueCodec[_, _, Map] = {
    pair match {
      case kv: Tuple2ValueCodec[k, v] @unchecked =>
        mapFromPair[k, v](kv)
    }
  }
}

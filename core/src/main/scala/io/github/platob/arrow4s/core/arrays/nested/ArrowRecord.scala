package io.github.platob.arrow4s.core.arrays.nested

import io.github.platob.arrow4s.core.codec.ValueCodec
import org.apache.arrow.vector.types.pojo.Field

abstract class ArrowRecord {
  def length: Int

  def fields: Seq[Field]

  def codecs: Seq[ValueCodec[_]]

  @inline def apply(index: Int): Any = get(index)

  @inline def get(index: Int): Any

  @inline def getAs[T](index: Int): T = get(index).asInstanceOf[T]

  @inline def unsafeSet(index: Int, value: Any): Unit

  def toArray: Array[Any] = {
    Array.tabulate(length)(i => get(i))
  }
}

object ArrowRecord {
  class Standalone(
    val fields: Seq[Field],
    val codecs: Seq[ValueCodec[_]],
    val values: Array[Any]
  ) extends ArrowRecord {
    override val length: Int = values.length

    override def get(i: Int): Any = values(i)

    override def unsafeSet(i: Int, value: Any): Unit = values(i) = value
  }

  class View(
    val array: StructArray[_],
    val index: Int
  ) extends ArrowRecord {
    override val length: Int = array.children.length

    def fields: Seq[Field] = array.children.map(_.field)

    def codecs: Seq[ValueCodec[_]] = array.children.map(_.codec)

    override def get(i: Int): Any =
      array.childAt(i).get(index)

    override def unsafeSet(i: Int, value: Any): Unit =
      array.childAt(i).unsafeSet(index, value)
  }

  @inline def standalone(fields: Seq[Field], codecs: Seq[ValueCodec[_]], values: Array[Any]): ArrowRecord =
    new Standalone(fields, codecs, values)

  @inline def view(array: StructArray[_], index: Int): ArrowRecord = new View(array, index)
}
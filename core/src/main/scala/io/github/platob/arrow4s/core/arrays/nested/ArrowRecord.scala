package io.github.platob.arrow4s.core.arrays.nested

import scala.reflect.runtime.{universe => ru}

trait ArrowRecord extends scala.collection.IndexedSeq[Any] {
  @inline def apply(index: Int): Any = getAny(index)

  @inline def getAny(index: Int): Any

  @inline def getAs(index: Int, tpe: ru.Type): Any

  @inline def getAs[T](index: Int)(implicit tt: ru.TypeTag[T]): T = {
    getAs(index, tt.tpe).asInstanceOf[T]
  }

  @inline def setAny(index: Int, value: Any): Unit

  @inline def setAs(index: Int, value: Any, tpe: ru.Type): Unit

  @inline def setAs[T](index: Int, value: T)(implicit tt: ru.TypeTag[T]): Unit = {
    setAs(index, value, tt.tpe)
  }
}

object ArrowRecord {
  @inline def view(array: StructArray[_], index: Int): ArrowRecord = {
    new ArrowRecord {
      override val length: Int = array.cardinality

      override def getAny(i: Int): Any = array.child(i).get(index)

      override def getAs(i: Int, tpe: ru.Type): Any = array.child(i).as(tpe).get(index)

      override def setAny(i: Int, value: Any): Unit = array.child(i).unsafeSet(index, value)

      override def setAs(i: Int, value: Any, tpe: ru.Type): Unit = array.child(i).as(tpe).unsafeSet(index, value)
    }
  }
}
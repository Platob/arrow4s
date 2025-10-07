package io.github.platob.arrow4s.core.reflection

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.{Type, TypeTag, typeOf}
import scala.reflect.runtime.{universe => ru}

object ReflectUtils {
  def isOption(tpe: Type): Boolean =
    tpe.typeConstructor =:= typeOf[Option[_]].typeConstructor

  def implements(tpe: Type, interface: Type): Boolean = {
    tpe.baseClasses.exists(_.asType.toType =:= interface)
  }

  private def implementsConstructor(tpe: Type, interface: Type): Boolean = {
    val ctr = interface.typeConstructor
    tpe.baseClasses.exists(_.asType.toType.typeConstructor =:= ctr)
  }

  def isTuple(tpe: Type): Boolean =
    tpe.typeSymbol.fullName.startsWith("scala.Tuple")

  def isIterable(tpe: Type): Boolean = {
    implementsConstructor(tpe, typeOf[Iterable[_]]) ||
      implementsConstructor(tpe, typeOf[Array[_]])
  }

  def isMap(tpe: Type): Boolean =
    implementsConstructor(tpe, typeOf[collection.Map[_, _]])

  def isSortedMap(tpe: Type): Boolean =
    implementsConstructor(tpe, typeOf[collection.SortedMap[_, _]])

  def typeArgument(tpe: Type, index: Int): Type =
    tpe.typeArgs(index)

  def defaultName[T : TypeTag]: String =
    defaultName(typeOf[T])

  /**
   * Default field name for a given type to camelCase the type name.
   * @param tpe the type
   * @return
   */
  def defaultName(tpe: Type): String = {
    val base = tpe.typeSymbol.name.decodedName.toString

    s"${base.head.toLower}${base.tail}" // make first letter lowercase: Base
  }

  def classTag[T](tpe: Type): ClassTag[T] = {
    implicit val m: ru.Mirror = ru.runtimeMirror(getClass.getClassLoader)

    ClassTag(m.runtimeClass(tpe.erasure)).asInstanceOf[ClassTag[T]]
  }
}

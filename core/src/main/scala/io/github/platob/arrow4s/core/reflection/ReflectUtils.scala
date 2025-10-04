package io.github.platob.arrow4s.core.reflection

import scala.reflect.runtime.universe.{Type, TypeTag, typeOf}

object ReflectUtils {
  def isOption(tpe: Type): Boolean =
    tpe.typeConstructor =:= typeOf[Option[_]].typeConstructor

  def implements(tpe: Type, interface: Type): Boolean = {
    tpe.baseClasses.exists(_.asType.toType =:= interface)
  }

  def isProduct(tpe: Type): Boolean =
    tpe.typeSymbol.isClass && tpe.typeSymbol.asClass.isCaseClass

  def isSeqLike(tpe: Type): Boolean = {
    val tc = tpe.typeConstructor
    tc =:= typeOf[List[_]].typeConstructor     ||
      tc =:= typeOf[Seq[_]].typeConstructor      ||
      tc =:= typeOf[Vector[_]].typeConstructor   ||
      tc =:= typeOf[Array[_]].typeConstructor    ||
      tc =:= typeOf[Iterable[_]].typeConstructor
  }

  def isMap(tpe: Type): Boolean =
    tpe.typeConstructor =:= typeOf[scala.collection.Map[_, _]].typeConstructor

  def isSortedMap(tpe: Type): Boolean =
    tpe.typeConstructor =:= typeOf[collection.immutable.SortedMap[_, _]].typeConstructor

  def getType[T: TypeTag]: Type =
    typeOf[T]

  def getTypeArgs(tpe: Type): List[Type] =
    tpe.typeArgs

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
}

package io.github.platob.arrow4s.core.reflection

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.{Type, typeOf}
import scala.reflect.runtime.{universe => ru}

object ReflectUtils {
  /**
   * Check if a type is an Option and return the inner type if so.
   * @param tpe the type to check
   * @return
   */
  @inline def unwrapNullable(tpe: Type): (Boolean, Type) =
    if (tpe =:= typeOf[Option[_]])
      (true, tpe.typeArgs.head)
    else if (tpe <:< typeOf[Some[_]])
      (false, tpe.typeArgs.head)
    else
      (false, tpe)

  @inline def isOption(tpe: Type): Boolean =
    tpe.typeConstructor =:= typeOf[Option[_]].typeConstructor

  private def implementsConstructor(tpe: Type, interface: Type): Boolean = {
    val ctr = interface.typeConstructor

    tpe.baseClasses.exists(_.asType.toType.typeConstructor =:= ctr)
  }

  @inline def isIterable(tpe: Type): Boolean = {
    implementsConstructor(tpe, typeOf[Iterable[_]]) ||
      implementsConstructor(tpe, typeOf[Array[_]])
  }

  @inline def isMap(tpe: Type): Boolean =
    implementsConstructor(tpe, typeOf[collection.Map[_, _]])

  @inline def isSortedMap(tpe: Type): Boolean =
    implementsConstructor(tpe, typeOf[collection.SortedMap[_, _]])

  /** Build a TypeTag[T] from a ru.Type, using the given mirror. */
  def typeTagFromType[T](tpe: ru.Type)(implicit m: ru.Mirror = ru.runtimeMirror(getClass.getClassLoader)): ru.TypeTag[T] = {
    val tc = new scala.reflect.api.TypeCreator {
      def apply[U <: scala.reflect.api.Universe with Singleton](mu: scala.reflect.api.Mirror[U]): U#Type =
        if (mu eq m) tpe.asInstanceOf[U#Type]
        else throw new IllegalArgumentException(s"Type tag defined in $m cannot be migrated to other mirrors.")
    }
    ru.TypeTag[T](m, tc)
  }

  def classTagFromTypeErased[T](tpe: ru.Type)(implicit m: ru.Mirror = ru.runtimeMirror(getClass.getClassLoader)): ClassTag[T] = {
    val cls = m.runtimeClass(tpe.erasure)
    ClassTag[T](cls) // note: generic args erased
  }
}

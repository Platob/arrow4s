package io.github.platob.arrow4s.core.codec.value

import io.github.platob.arrow4s.core.codec.value.nested.StructValueCodec

import scala.collection.concurrent.TrieMap
import scala.reflect.runtime.{universe => ru}

object ValueCodecRegistry {
  // -- Runtime registry: map ru.Type -> ValueCodec[_]
  private val _registry = TrieMap.empty[ru.Type, ValueCodec[_]]

  private def isCtr(tpe: ru.Type, target: ru.Type): Boolean =
    tpe.typeConstructor =:= target.typeConstructor

  def apply(tpe: ru.Type): ValueCodec[_] =
    _registry.getOrElseUpdate(tpe, {
      if (isCtr(tpe, ru.typeOf[Option[_]])) {
        // Handle Option[T] specially
        val argType = tpe.typeArgs.head
        val argCodec = apply(argType)
        val built = argCodec.toOptionalCodec

        // Register the built codec for future use
        _registry.put(tpe, built)

        built
      } else {
        val built = StructValueCodec.unsafe(tpe)

        // Register the built codec for future use
        _registry.put(tpe, built)

        built
      }
    })

  def get(tpe: ru.Type): Option[ValueCodec[_]] =
    _registry.get(tpe)

  def getOrElse(tpe: ru.Type, orElse: => ValueCodec[_]): ValueCodec[_] =
    _registry.getOrElse(tpe, orElse)

  def getOrElseUpdate(tpe: ru.Type, orElse: => ValueCodec[_]): ValueCodec[_] =
    _registry.getOrElseUpdate(tpe, orElse)

  def register[T](c: ValueCodec[T]): Unit =
    _registry.put(c.tpe, c)
}

package io.github.platob.arrow4s.core.extensions

import io.github.platob.arrow4s.core.reflection.ReflectUtils
import io.github.platob.arrow4s.core.types.ArrowField
import org.apache.arrow.vector.types.pojo.Field

import java.lang.invoke.{MethodHandle, MethodHandles}
import java.util.concurrent.ConcurrentHashMap
import scala.reflect.runtime.{universe => ru}

object FastCtorCache {
  final case class CodecField[T](
    name: String,
    tpe: ru.Type,
    index: Int,          // 0-based position in the primary-ctor param list (flattened)
    default: Option[T]   // default *for that field*, if any
  )

  final case class Codec(
    ctor: MethodHandle,
    fields: Vector[CodecField[Any]],
    tuple: Boolean
  ) {
    // Arrow schema fields (names + types)
    def arrowFields: Seq[Field] =
      fields.map(cf => ArrowField.fromScala(cf.tpe, cf.name))
  }

  // cache by *ru.Type* so generic instantiations don’t collide (e.g., Foo[Int] vs Foo[String])
  private[this] val cache  = new ConcurrentHashMap[ru.Type, Codec]()
  private[this] val lookup = MethodHandles.lookup()

  def codecFor(tpe: ru.Type): Codec = {
    val hit = cache.get(tpe)
    if (hit != null) return hit

    val mirror = ru.runtimeMirror(getClass.getClassLoader)
    val cls    = mirror.runtimeClass(tpe)

    val codec: Codec =
      if (ReflectUtils.isTuple(tpe)) {
        // ---------- Tuple branch ----------
        val typeArgs = tpe.typeArgs
        val n = typeArgs.size
        require(n > 0, s"Unit/empty tuple not supported as a struct: $tpe")

        // JVM ctor is (Object, ..., Object)
        val objParams = Array.fill[Class[_]](n)(classOf[Object])
        val jctor =
          try cls.getDeclaredConstructor(objParams: _*)
          catch { case _: NoSuchMethodException => cls.getConstructor(objParams: _*) }
        if (!jctor.isAccessible) jctor.setAccessible(true)
        val mh = lookup.unreflectConstructor(jctor)

        // Tuples don’t have parameter defaults; index is 0-based order
        val cfs =
          (0 until n).iterator.map { i =>
            CodecField[Any](name = s"_${i + 1}", tpe = typeArgs(i), index = i, default = None)
          }.toVector

        Codec(ctor = mh, fields = cfs, tuple = true)

      } else {
        // ---------- Case class / Product with primary ctor ----------
        val classSym = tpe.typeSymbol.asClass
        require(
          classSym.isCaseClass || (tpe <:< ru.typeOf[Product]),
          s"Unsupported struct type (need case class or tuple): $tpe"
        )

        val ctorSym   = classSym.primaryConstructor.asMethod
        val ctorParams = ctorSym.paramLists.flatten

        if (ctorParams.isEmpty) {
          throw new IllegalArgumentException(s"Cannot create struct codec for type with no constructor parameters: $tpe")
        }

        // Param names and instantiated types
        val fields: Vector[(String, ru.Type)] =
          ctorParams.iterator.zipWithIndex.map { case (p, _) =>
            val name = p.name.decodedName.toString
            val pt   = p.typeSignatureIn(tpe) // instantiated type
            name -> pt
          }.toVector

        // JVM constructor param classes
        val jParamClasses: Seq[Class[_]] =
          fields.map { case (_, pt) => mirror.runtimeClass(pt) }

        val jctor =
          try cls.getDeclaredConstructor(jParamClasses: _*)
          catch { case _: NoSuchMethodException => cls.getConstructor(jParamClasses: _*) }
        if (!jctor.isAccessible) jctor.setAccessible(true)
        val mh = lookup.unreflectConstructor(jctor)

        // ----- Per-field defaults via companion's apply$default$N -----
        // We compute Option[Any] per param index. Missing getter => None.
        // ----- Per-field defaults via companion's apply$default$N, with Option[_] => None fallback -----
        val defaults: Vector[Option[Any]] = {
          val ctorParams = classSym.primaryConstructor.asMethod.paramLists.flatten
          val companionSym = classSym.companion

          val (compIMOpt, compTypeOpt) =
            if (companionSym == ru.NoSymbol) (None, None)
            else {
              val module    = companionSym.asModule
              val modMirror = mirror.reflectModule(module)
              val companion = modMirror.instance
              (Some(mirror.reflect(companion)), Some(module.typeSignature))
            }

          ctorParams.zipWithIndex.map { case (p, i) =>
            val paramHasDefault = p.asTerm.isParamWithDefault
            val fieldTpe        = p.typeSignatureIn(tpe)

            if (paramHasDefault && compIMOpt.isDefined && compTypeOpt.isDefined) {
              // try the real Scala default: apply$default$N
              val getterName = ru.TermName(s"apply$$default$$${i + 1}")
              val sym        = compTypeOpt.get.member(getterName)
              if (sym != ru.NoSymbol && sym.isMethod) {
                try Some(compIMOpt.get.reflectMethod(sym.asMethod)())
                catch { case _: Throwable => if (ReflectUtils.isOption(fieldTpe)) Some(None) else None }
              } else {
                // no getter even though isParamWithDefault said yes — fall back to Option None if relevant
                if (ReflectUtils.isOption(fieldTpe)) Some(None) else None
              }
            } else {
              // no ctor default => give Option fields a default None
              if (ReflectUtils.isOption(fieldTpe)) Some(None) else None
            }
          }.toVector
        }

        val cfs: Vector[CodecField[Any]] =
          fields.zipWithIndex.map { case ((name, pt), idx) =>
            CodecField[Any](name = name, tpe = pt, index = idx, default = defaults(idx))
          }

        Codec(ctor = mh, fields = cfs, tuple = false)
      }

    cache.put(tpe, codec)
    codec
  }
}

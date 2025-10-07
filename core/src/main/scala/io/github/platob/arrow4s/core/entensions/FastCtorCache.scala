package io.github.platob.arrow4s.core.entensions

import io.github.platob.arrow4s.core.reflection.ReflectUtils

import java.lang.invoke.{MethodHandle, MethodHandles}
import java.util.concurrent.ConcurrentHashMap
import scala.reflect.runtime.{universe => ru}

private[core] object FastCtorCache {
  final case class Codec(
    ctor: MethodHandle,
    fields: Seq[(String, ru.Type)], // ordered: (paramName, paramTypeInT)
    tuple: Boolean
  )

  // cache by *ru.Type* so generic instantiations don’t collide (e.g., Foo[Int] vs Foo[String])
  private[this] val cache = new ConcurrentHashMap[ru.Type, Codec]()
  private[this] val lookup = MethodHandles.lookup()

  def codecFor(tpe: ru.Type): Codec = {
    val hit = cache.get(tpe)
    if (hit != null) return hit

    val mirror = ru.runtimeMirror(getClass.getClassLoader)
    val cls    = mirror.runtimeClass(tpe)

    val codec: Codec =
      if (ReflectUtils.isTuple(tpe)) {
        // --- Tuple branch ---
        val typeArgs = tpe.typeArgs
        val n = typeArgs.size
        require(n > 0, s"Unit/empty tuple not supported as a struct: $tpe")

        // names: _1, _2, ... _n ; types: the instantiated type args
        val fields: Seq[(String, ru.Type)] =
          (1 to n).iterator.map(i => s"_$i" -> typeArgs(i - 1)).toVector

        // TupleN ctors erase to (Object, ..., Object)
        val objParams = Array.fill[Class[_]](n)(classOf[Object])
        val jctor =
          try cls.getDeclaredConstructor(objParams: _*)
          catch {
            // fallback: some toolchains expose only public ctor via getConstructor
            case _: NoSuchMethodException => cls.getConstructor(objParams: _*)
          }
        if (!jctor.isAccessible) jctor.setAccessible(true)
        val mh = lookup.unreflectConstructor(jctor)

        Codec(ctor = mh, fields = fields, tuple = true)
      } else {
        // --- Case class / regular Product with primary ctor ---
        val classSym = tpe.typeSymbol.asClass
        require(classSym.isCaseClass || (tpe <:< ru.typeOf[Product]),
          s"Unsupported struct type (need case class or tuple): $tpe"
        )
        val ctorSym = classSym.primaryConstructor.asMethod
        val params  = ctorSym.paramLists.flatten

        val fields: Seq[(String, ru.Type)] =
          params.map { p =>
            val name = p.name.decodedName.toString
            val pt   = p.typeSignatureIn(tpe) // instantiated type
            name -> pt
          }

        val jParamClasses: Seq[Class[_]] =
          fields.map { case (_, pt) => mirror.runtimeClass(pt) }

        val jctor =
          try cls.getDeclaredConstructor(jParamClasses: _*)
          catch {
            case _: NoSuchMethodException => cls.getConstructor(jParamClasses: _*)
          }
        if (!jctor.isAccessible) jctor.setAccessible(true)
        val mh = lookup.unreflectConstructor(jctor)

        Codec(ctor = mh, fields = fields, tuple = false)
      }

    cache.put(tpe, codec)
    codec
  }
}

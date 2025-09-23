package io.github.platob.arrow4s.core.types

import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType}

import scala.jdk.CollectionConverters.{MapHasAsJava, SeqHasAsJava}
import scala.reflect.runtime.universe.{Type, typeOf, TypeTag, termNames}

object ArrowField {
  /** Hook signature:
   *  - (t, name, nullable, next) → Option[Field]
   *  - return Some(field) to handle; None to fall back to default.
   *  - `next` lets your hook recurse with the default rules.
   */
  type Hook = (Type, String, Boolean, Option[Map[String, String]]) => Option[Field]

  val noHook: Hook = (_, _, _, _) => None

  private def build(
    name: String,
    at: ArrowType,
    nullable: Boolean,
    children: List[Field],
    metadata: Option[Map[String, String]]
  ): Field = {
    val fieldType = new FieldType(nullable, at, null, metadata.map(_.asJava).orNull)

    new Field(name, fieldType, children.asJava)
  }

  def fromScala[T](implicit tt: TypeTag[T]): Field =
    fromScala[T](name = defaultName(typeOf[T]))

  def fromScala[T](name: String)(implicit tt: TypeTag[T]): Field =
    fromScala[T](
      name = name,
      metadata = None,
      hook = noHook
    )

  def fromScala[T](
    name: String,
    metadata: Option[Map[String, String]],
    hook: Hook
  )(implicit tt: TypeTag[T]): Field = {
    val tpe = typeOf[T]

    fromScala(
      tpe,
      name,
      nullable = false,
      metadata,
      hook = hook
    )
  }

  def fromScala(
    tpe0: Type,
    name: String,
    nullable: Boolean,
    metadata: Option[Map[String, String]],
    hook: Hook
  ): Field = {
    val tpe = tpe0.dealias

    if (tpe.typeConstructor =:= typeOf[Option[_]].typeConstructor)
      return fromScala(tpe.typeArgs.head, name, nullable = true, metadata, hook)

    hook(tpe, name, nullable, metadata)
      .foreach(f => return f)

    if (tpe =:= typeOf[Int])
      build(name, new ArrowType.Int(32, true), nullable = nullable, metadata = metadata, children = Nil)
    else if (tpe =:= typeOf[Long])
      build(name, new ArrowType.Int(64, true), nullable = nullable, metadata = metadata, children = Nil)
    else if (tpe =:= typeOf[Short])
      build(name, new ArrowType.Int(16, true), nullable = nullable, metadata = metadata, children = Nil)
    else if (tpe =:= typeOf[Byte])
      build(name, new ArrowType.Int(8,  true), nullable = nullable, metadata = metadata, children = Nil)
    else if (tpe =:= typeOf[Boolean])
      build(name, new ArrowType.Bool(), nullable = nullable, metadata = metadata, children = Nil)
    else if (tpe =:= typeOf[Float])
      build(name, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), nullable = nullable, metadata = metadata, children = Nil)
    else if (tpe =:= typeOf[Double]) {
      build(
        name, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE),
        nullable = nullable, metadata = metadata, children = Nil
      )
    } else if (tpe =:= typeOf[String])
      build(name, new ArrowType.Utf8(), nullable = nullable, metadata = metadata, children = Nil)
    else if (tpe =:= typeOf[Array[Byte]])
      build(name, new ArrowType.Binary(), nullable = nullable, metadata = metadata, children = Nil)
    else if (isSeqLike(tpe)) {
      val child = fromScala(
        tpe.typeArgs.head,
        "item",
        nullable = false, metadata = None,
        hook
      )

      build(name, new ArrowType.List(), nullable = nullable, metadata = metadata, children = List(child))
    }
    else if (tpe.typeConstructor =:= typeOf[Map[_, _]].typeConstructor) {
      val keyField = fromScala(
        tpe.typeArgs.head, "key",
        nullable = false, metadata = None, hook
      )
      val valField = fromScala(
        tpe.typeArgs(1), "value",
        nullable = true, metadata = None, hook
      )

      val entries  = build(
        "entries", new ArrowType.Struct(),
        nullable = nullable, metadata = None,
        children = List(keyField, valField)
      )

      build(
        name, new ArrowType.Map(false),
        nullable = nullable, metadata = metadata,
        children = List(entries)
      )
    }
    else if (tpe.typeSymbol.fullName.startsWith("scala.Tuple")) {
      val kids = tpe.typeArgs.zipWithIndex.map {
        case (a, i) => fromScala(
          a, s"_${i+1}",
          nullable = false, metadata = None, hook
        )
      }

      build(name, new ArrowType.Struct(), nullable = nullable, metadata = metadata, children = kids)
    }
    else if (tpe.typeSymbol.isClass && tpe.typeSymbol.asClass.isCaseClass) {
      val ctor   = tpe.decl(termNames.CONSTRUCTOR).asMethod
      val params = ctor.paramLists.flatten
      val kids = params.map { p =>
        fromScala(
          p.typeSignatureIn(tpe).resultType,
          p.name.decodedName.toString,
          nullable = false, metadata = None,
          hook
        )
      }

      val m = metadata.getOrElse(Map.empty) ++
        Map("namespace" -> tpe.typeSymbol.fullName)

      build(name, new ArrowType.Struct(), nullable = nullable, metadata = Some(m), children = kids)
    }
    else {
      throw new IllegalArgumentException(s"Unsupported Scala type for Arrow conversion: $tpe")
    }
  }

  private def isSeqLike(tpe: Type): Boolean = {
    val tc = tpe.typeConstructor
    tc =:= typeOf[List[_]].typeConstructor     ||
      tc =:= typeOf[Seq[_]].typeConstructor      ||
      tc =:= typeOf[Vector[_]].typeConstructor   ||
      tc =:= typeOf[Array[_]].typeConstructor    ||
      tc =:= typeOf[Iterable[_]].typeConstructor
  }

  /**
   * Default field name for a given type to camelCase the type name.
   * @param tpe the type
   * @return
   */
  private def defaultName(tpe: Type): String = {
    val base = tpe.typeSymbol.name.decodedName.toString

    s"${base.head.toLower}${base.tail}" // make first letter lowercase: Base
  }
}

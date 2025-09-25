package io.github.platob.arrow4s.core.types

import io.github.platob.arrow4s.core.values.{UByte, UInt, ULong, UShort}
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType}

import scala.jdk.CollectionConverters.{MapHasAsJava, SeqHasAsJava}
import scala.reflect.runtime.universe.{Type, TypeTag, termNames, typeOf}

object ArrowField {
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

  def fromScala[T : TypeTag]: Field =
    fromScala[T](name = defaultName(typeOf[T]))

  def fromScala[T : TypeTag](name: String): Field =
    fromScala[T](
      name = name,
      metadata = None,
    )

  def fromScala[T : TypeTag](
    name: String,
    metadata: Option[Map[String, String]],
  ): Field = {
    val tpe = typeOf[T]

    fromScala(
      tpe,
      name,
      nullable = false,
      metadata,
    )
  }

  def fromScala(tpe: Type, name: String): Field = {
    val (nullable, baseType) =
      if (isOption(tpe))
        (true, tpe.typeArgs.head)
      else
        (false, tpe)

    fromScala(
      baseType,
      name = name,
      nullable = nullable,
      metadata = None,
    )
  }

  def fromScala(
    tpe0: Type,
    name: String,
    nullable: Boolean,
    metadata: Option[Map[String, String]],
  ): Field = {
    val tpe = tpe0.dealias

    if (isOption(tpe))
      return fromScala(tpe.typeArgs.head, name, nullable = true, metadata)

    if (tpe =:= typeOf[Byte])
      build(name, new ArrowType.Int(8,  true), nullable = nullable, metadata = metadata, children = Nil)
    else if (tpe =:= typeOf[UByte])
      build(name, new ArrowType.Int(8,  false), nullable = nullable, metadata = metadata, children = Nil)
    else if (tpe =:= typeOf[Short])
      build(name, new ArrowType.Int(16, true), nullable = nullable, metadata = metadata, children = Nil)
    else if (tpe =:= typeOf[UShort])
      build(name, new ArrowType.Int(16, false), nullable = nullable, metadata = metadata, children = Nil)
    else if (tpe =:= typeOf[Int])
      build(name, new ArrowType.Int(32, true), nullable = nullable, metadata = metadata, children = Nil)
    else if (tpe =:= typeOf[UInt])
      build(name, new ArrowType.Int(32, false), nullable = nullable, metadata = metadata, children = Nil)
    else if (tpe =:= typeOf[Long])
      build(name, new ArrowType.Int(64, true), nullable = nullable, metadata = metadata, children = Nil)
    else if (tpe =:= typeOf[ULong])
      build(name, new ArrowType.Int(64, false), nullable = nullable, metadata = metadata, children = Nil)
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
      )

      build(name, new ArrowType.List(), nullable = nullable, metadata = metadata, children = List(child))
    }
    else if (tpe.typeConstructor =:= typeOf[Map[_, _]].typeConstructor) {
      val keyField = fromScala(
        tpe.typeArgs.head, "key",
        nullable = false, metadata = None
      )
      val valField = fromScala(
        tpe.typeArgs(1), "value",
        nullable = true, metadata = None
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
          nullable = false, metadata = None
        )
      }

      build(name, new ArrowType.Struct(), nullable = nullable, metadata = metadata, children = kids)
    }
    else if (isProduct(tpe)) {
      val ctor   = tpe.decl(termNames.CONSTRUCTOR).asMethod
      val params = ctor.paramLists.flatten
      val kids = params.map { p =>
        fromScala(
          p.typeSignatureIn(tpe).resultType,
          p.name.decodedName.toString,
          nullable = false, metadata = None,
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

  def isOption(tpe: Type): Boolean =
    tpe.typeConstructor =:= typeOf[Option[_]].typeConstructor

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

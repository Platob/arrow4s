package io.github.platob.arrow4s.core.types

import io.github.platob.arrow4s.core.reflection.ReflectUtils
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
    fromScala[T](name = ReflectUtils.defaultName(typeOf[T]))

  def fromScala[T : TypeTag](name: String): Field =
    fromScala[T](
      name = name,
      metadata = None,
    )

  def fromScala[T : TypeTag](
    name: String,
    metadata: Option[Map[String, String]],
  ): Field = {
    val tpe = typeOf[T].dealias

    fromScala(
      tpe,
      name,
      nullable = false,
      metadata,
    )
  }

  def fromScala(tpe: Type, name: String): Field = {
    val (nullable, baseType) =
      if (ReflectUtils.isOption(tpe))
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

    if (ReflectUtils.isOption(tpe))
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
    else if (ReflectUtils.isSeqLike(tpe)) {
      val child = fromScala(
        tpe.typeArgs.head,
        "item",
        nullable = false, metadata = None,
      )

      build(name, new ArrowType.List(), nullable = nullable, metadata = metadata, children = List(child))
    }
    else if (ReflectUtils.isMap(tpe)) {
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

      // Check if TreeMap
      val keySorted = ReflectUtils.isSortedMap(tpe)

      build(
        name, new ArrowType.Map(keySorted),
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

      val m = metadata.getOrElse(Map.empty) ++
        Map("namespace" -> tpe.typeSymbol.fullName)

      build(name, new ArrowType.Struct(), nullable = nullable, metadata = Some(m), children = kids)
    }
    else if (ReflectUtils.isProduct(tpe)) {
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
}

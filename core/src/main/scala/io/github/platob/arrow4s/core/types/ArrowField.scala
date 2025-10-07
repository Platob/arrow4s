package io.github.platob.arrow4s.core.types

import io.github.platob.arrow4s.core.extensions.FastCtorCache
import io.github.platob.arrow4s.core.reflection.ReflectUtils
import io.github.platob.arrow4s.core.values.{UByte, UInt, ULong}
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType}

import scala.jdk.CollectionConverters.{MapHasAsJava, SeqHasAsJava}
import scala.reflect.runtime.universe.{Type, TypeTag, typeOf}

object ArrowField {
  def build(
    name: String,
    at: ArrowType,
    nullable: Boolean,
    children: Seq[Field],
    metadata: Option[Map[String, String]]
  ): Field = {
    val fieldType = new FieldType(nullable, at, null, metadata.map(_.asJava).orNull)

    new Field(name, fieldType, children.asJava)
  }

  def fromScala[T](implicit tt: TypeTag[T]): Field =
    fromScala[T](name = ReflectUtils.defaultName(typeOf[T]))

  def fromScala[T](name: String)(implicit tt: TypeTag[T]): Field =
    fromScala[T](
      name = name,
      metadata = None,
    )

  def fromScala[T](
    name: String,
    metadata: Option[Map[String, String]],
  )(implicit tt: TypeTag[T]): Field = {
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
    tpe: Type,
    name: String,
    nullable: Boolean,
    metadata: Option[Map[String, String]],
  ): Field = {
    if (ReflectUtils.isOption(tpe))
      return fromScala(tpe.typeArgs.head, name, nullable = true, metadata)

    if (tpe =:= typeOf[Byte])
      build(name, new ArrowType.Int(8,  true), nullable = nullable, metadata = metadata, children = Nil)
    else if (tpe =:= typeOf[UByte])
      build(name, new ArrowType.Int(8,  false), nullable = nullable, metadata = metadata, children = Nil)
    else if (tpe =:= typeOf[Short])
      build(name, new ArrowType.Int(16, true), nullable = nullable, metadata = metadata, children = Nil)
    else if (tpe =:= typeOf[Char])
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
      build(name, ArrowType.Bool.INSTANCE, nullable = nullable, metadata = metadata, children = Nil)
    else if (tpe =:= typeOf[Float])
      build(name, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), nullable = nullable, metadata = metadata, children = Nil)
    else if (tpe =:= typeOf[Double]) {
      build(
        name, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE),
        nullable = nullable, metadata = metadata, children = Nil
      )
    } else if (tpe =:= typeOf[String])
      build(name, ArrowType.Utf8.INSTANCE, nullable = nullable, metadata = metadata, children = Nil)
    else if (tpe =:= typeOf[Array[Byte]])
      build(name, ArrowType.Binary.INSTANCE, nullable = nullable, metadata = metadata, children = Nil)
    else if (ReflectUtils.isMap(tpe)) {
      val keyField = fromScala(
        tpe.typeArgs.head, "key",
        nullable = false, metadata = None
      )
      val valField = fromScala(
        tpe.typeArgs(1), "value",
        nullable = false, metadata = None
      )

      val entries  = build(
        "entries",
        ArrowType.Struct.INSTANCE,
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
    else if (ReflectUtils.isIterable(tpe)) {
      val child = fromScala(
        tpe.typeArgs.head,
        "item",
        nullable = false, metadata = None,
      )

      build(
        name, ArrowType.List.INSTANCE,
        nullable = nullable,
        metadata = metadata, children = List(child)
      )
    }
    else {
      val codec = FastCtorCache.codecFor(tpe)
      val children = codec.arrowFields

      val m = metadata.getOrElse(Map.empty) ++
        Map("namespace" -> tpe.typeSymbol.fullName)

      build(
        name, ArrowType.Struct.INSTANCE, nullable = nullable, metadata = Some(m),
        children = children
      )
    }
  }
}

package io.github.platob.arrow4s.core.codec.value.nested

import io.github.platob.arrow4s.core.codec.value.{ValueCodec, ValueCodecRegistry}
import io.github.platob.arrow4s.core.reflection.ReflectUtils
import io.github.platob.arrow4s.core.types.ArrowField
import org.apache.arrow.vector.types.pojo.Field

import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}

abstract class StructValueCodec[P](
  val arrowField: Field,
  val children: Seq[ValueCodec[_]],
  tpe: ru.Type,
  typeTag: ru.TypeTag[P],
  clsTag: ClassTag[P]
) extends NestedValueCodec[P](tpe = tpe, typeTag = typeTag, clsTag = clsTag) {
  override def namespace: String = tpe.typeSymbol.fullName

//  override def arrowField: Field = {
//    val fields = children.map(_.arrowField)
//
//    ArrowField.build(
//      name = namespace,
//      at = ArrowType.Struct.INSTANCE,
//      nullable = false,
//      children = fields,
//      metadata = Some(Map("namespace" -> namespace))
//    )
//  }
}

object StructValueCodec {
  implicit def product[P <: Product](implicit tt: ru.TypeTag[P], ct: ClassTag[P]): TupleValueCodec[P] = {
    val tpe = tt.tpe

    // Check if Tuple
    val isTuple = tpe.typeSymbol.fullName.startsWith("scala.Tuple")
    if (isTuple) {
      val c = tpe.typeArgs.map(ValueCodecRegistry.apply)

      return TupleValueCodec.tuple(c).asInstanceOf[TupleValueCodec[P]]
    }

    TupleValueCodec.tuple(Seq.empty).asInstanceOf[TupleValueCodec[P]]

    // --- Collect fields (tuple => from type args; case class => from accessors)
//    val children: Seq[(org.apache.arrow.vector.types.pojo.Field, ValueCodec[_])] =
//      tpe.decls.collect {
//        case m: ru.MethodSymbol if m.isCaseAccessor =>
//          val fType = m.returnType
//          val fName = m.name.toString
//          val codec = ValueCodecRegistry(fType)
//          val field = ArrowField.rename(codec.arrowField, fName)
//          (field, codec)
//      }.toSeq
//
//    val arrowField = ArrowField.struct(
//      name = tpe.typeSymbol.name.toString,
//      children = children.map(_._1),
//      nullable = false,
//      metadata = Some(Map("namespace" -> tpe.typeSymbol.fullName))
//    )

    // --- Make constructor from Array[Any] -> P (cached) ---
//    val mirror      = ru.runtimeMirror(ct.runtimeClass.getClassLoader)
//    val classSymbol = tpe.typeSymbol.asClass
//    val ctorSym     = classSymbol.primaryConstructor.asMethod
//    val classMirror = mirror.reflectClass(classSymbol)
//    val ctorMirror  = classMirror.reflectConstructor(ctorSym)
//
//    // a fast function we can call repeatedly
//    val ctor: Array[Any] => P = (values: Array[Any]) =>
//      ctorMirror(values.toSeq: _*).asInstanceOf[P]
//    // ----------------------------------------------- :contentReference[oaicite:0]{index=0}
//
//    new StructValueCodec[P](arrowField, children.map(_._2), tpe, tt, ct) {
//      override def fromElements(values: Array[Any]): P =
//        ctor(values) // values must match primary ctor param order
//
//      override def elementAt[Elem](value: P, index: Int): Elem =
//        value.productElement(index).asInstanceOf[Elem]
//    }
  }

  def unsafe(tpe: ru.Type): StructValueCodec[_] = {
    val tt = ReflectUtils.typeTagFromType(tpe)           // ru.TypeTag[_]
    val ct = ReflectUtils.classTagFromTypeErased(tpe)    // ClassTag[_]

    // Instantiate via a wider P (Product). The TypeTag/ClassTag still point to the exact case-class.
    StructValueCodec.product[Product](
      tt.asInstanceOf[ru.TypeTag[Product]],
      ct.asInstanceOf[ClassTag[Product]]
    )
  }

  def fromChildren(children: Seq[ValueCodec[_]]): StructValueCodec[_] = {
    TupleValueCodec.tuple(children = children)
  }
}
package io.github.platob.arrow4s.core.types

import io.github.platob.arrow4s.core.types.ArrowFieldSuite.Person
import munit.FunSuite
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.pojo.{ArrowType, Field}

import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.reflect.runtime.universe._

class ArrowFieldSuite extends FunSuite {

  private def children(f: Field): List[Field] =
    f.getChildren.asScala.toList

  test("build a non-nullable Int32 when given Int") {
    val f = ArrowField.fromScala[Int](name = "i32")
    assertEquals(f.getName, "i32")
    assertEquals(f.isNullable, false)
    assertEquals(f.getType, new ArrowType.Int(32, /*signed*/ true))
    assertEquals(children(f), Nil)
  }

  test("build a nullable Double when given Option[Double]") {
    val f = ArrowField.fromScala[Option[Double]](name = "dopt")
    assertEquals(f.getName, "dopt")
    assertEquals(f.isNullable, true)
    assertEquals(f.getType, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE))
  }

  test("build a List(Utf8) with child named 'item' for Seq-like") {
    val f = ArrowField.fromScala[List[String]](name = "xs")
    assertEquals(f.getType, new ArrowType.List())
    assertEquals(f.isNullable, false)

    val item = children(f) match {
      case one :: Nil => one
      case other      => fail(s"Expected exactly one child, got: $other")
    }
    assertEquals(item.getName, "item")
    assertEquals(item.getType, new ArrowType.Utf8())
    // element nullability expectation per builder behavior
    assertEquals(item.isNullable, false)
  }

  test("build a Map(key:Utf8 non-null, value:Int32 nullable) with 'entries' struct") {
    val f = ArrowField.fromScala[Map[String, Int]](name = "m")
    assertEquals(f.getType, new ArrowType.Map(/*keysSorted*/ false))
    assertEquals(f.isNullable, false)

    val entries = children(f) match {
      case e :: Nil => e
      case other    => fail(s"Expected single 'entries' child, got: $other")
    }
    assertEquals(entries.getName, "entries")
    assertEquals(entries.getType, new ArrowType.Struct())
    assertEquals(entries.isNullable, false)

    val List(k, v) = children(entries)
    assertEquals(k.getName, "key")
    assertEquals(k.isNullable, false)
    assertEquals(k.getType, new ArrowType.Utf8())

    assertEquals(v.getName, "value")
    assertEquals(v.isNullable, true)
    assertEquals(v.getType, new ArrowType.Int(32, true))
  }

  test("build a Struct for tuples with numbered children") {
    val f = ArrowField.fromScala[(Int, String)](name = "pair")
    assertEquals(f.getType, new ArrowType.Struct())
    assertEquals(f.isNullable, false)

    val List(_1, _2) = children(f)
    assertEquals(_1.getName, "_1")
    assertEquals(_1.getType, new ArrowType.Int(32, true))
    assertEquals(_1.isNullable, false)

    assertEquals(_2.getName, "_2")
    assertEquals(_2.getType, new ArrowType.Utf8())
    assertEquals(_2.isNullable, false)
  }

  test("build a Struct for case classes with parameter names and option nullability") {
    val f = ArrowField.fromScala[Person]
    assertEquals(f.getType, new ArrowType.Struct())
    val List(nameF, ageF) = children(f)

    assertEquals(nameF.getName, "name")
    assertEquals(nameF.getType, new ArrowType.Utf8())
    assertEquals(nameF.isNullable, false)

    assertEquals(ageF.getName, "age")
    assertEquals(ageF.getType, new ArrowType.Int(32, true))
    assertEquals(ageF.isNullable, true)
  }

  test("respect explicit nullable flag when called via the (Type, name, nullable) overload") {
    val tpe = typeOf[Long]
    val f   = ArrowField.fromScala(tpe, name = "id", nullable = true, metadata = None, hook = ArrowField.noHook)
    assertEquals(f.getName, "id")
    assertEquals(f.isNullable, true)
    assertEquals(f.getType, new ArrowType.Int(64, true))
  }

  test("throw for unsupported types") {
    val ex = intercept[IllegalArgumentException] {
      // e.g., Either is not implemented in ArrowField.fromScala
      ArrowField.fromScala[Either[Int, Int]](name = "e")
    }
    assert(ex.getMessage.contains("Unsupported Scala type"))
  }
}

object ArrowFieldSuite {
  case class Person(name: String, age: Option[Int])
}

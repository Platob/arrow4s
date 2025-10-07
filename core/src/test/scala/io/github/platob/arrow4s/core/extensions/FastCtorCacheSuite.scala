package io.github.platob.arrow4s.core.extensions

import io.github.platob.arrow4s.core.extensions.FastCtorCacheSuite._
import munit.FunSuite

import scala.reflect.runtime.{universe => ru}

final class FastCtorCacheSuite extends FunSuite {
  // Helpers
  private def codecOf[T](implicit tt: ru.TypeTag[T]): FastCtorCache.Codec =
    FastCtorCache.codecFor(ru.typeOf[T])

  private def defaultsOf[T](implicit tt: ru.TypeTag[T]): Vector[Option[Any]] =
    codecOf[T].fields.map(_.default)

  private def namesOf[T](implicit tt: ru.TypeTag[T]): Vector[String] =
    codecOf[T].fields.map(_.name)

  private def tpesOf[T](implicit tt: ru.TypeTag[T]): Vector[ru.Type] =
    codecOf[T].fields.map(_.tpe)

  test("tuple2: names, types, and no per-field defaults") {
    val codec = codecOf[(Int, String)]
    assertEquals(codec.tuple, true)

    assertEquals(namesOf[(Int, String)], Vector("_1", "_2"))
    assertEquals(
      tpesOf[(Int, String)],
      Vector(ru.typeOf[Int], ru.typeOf[String])
    )
    assertEquals(defaultsOf[(Int, String)], Vector(None, None))
  }

  test("case class with all defaults: every field has Some(default)") {
    val codec = codecOf[AllDefaults]
    assertEquals(codec.tuple, false)

    assertEquals(namesOf[AllDefaults], Vector("a", "b", "c"))
    assertEquals(
      tpesOf[AllDefaults],
      Vector(ru.typeOf[Int], ru.typeOf[String], ru.typeOf[Boolean])
    )
    assertEquals(defaultsOf[AllDefaults], Vector(Some(1), Some("x"), Some(true)))
  }

  test("case class with partial defaults: mix of Some/None per position") {
    val codec = codecOf[PartialDefaults]
    assertEquals(codec.tuple, false)

    assertEquals(namesOf[PartialDefaults], Vector("i", "d", "o", "opt", "optNull"))
    assertEquals(
      tpesOf[PartialDefaults],
      Vector(ru.typeOf[Int], ru.typeOf[Double], ru.typeOf[Option[String]], ru.typeOf[Option[Int]], ru.typeOf[Option[Int]])
    )
    // i has default, d does not, o has default None
    assertEquals(defaultsOf[PartialDefaults], Vector(Some(42), None, Some(None), Some(Some(7)), Some(None)))
  }

  test("case class with no defaults: all None") {
    val codec = codecOf[NoDefaults]
    assertEquals(codec.tuple, false)

    assertEquals(namesOf[NoDefaults], Vector("x", "y"))
    assertEquals(
      tpesOf[NoDefaults],
      Vector(ru.typeOf[Long], ru.typeOf[Boolean])
    )
    assertEquals(defaultsOf[NoDefaults], Vector(None, None))
  }

  test("generic case class with default on an Option[A]: default is Some(None) after instantiation") {
    // instantiate with a concrete type arg to ensure tpe is specialized
    val codec = codecOf[GenericOptInt]
    assertEquals(codec.tuple, false)

    assertEquals(namesOf[GenericOptInt], Vector("x"))
    assertEquals(tpesOf[GenericOptInt], Vector(ru.typeOf[Option[Int]]))
    assertEquals(defaultsOf[GenericOptInt], Vector(Some(None)))
  }

  test("field indices are 0-based and ordered like primary constructor") {
    val codec = codecOf[PartialDefaults]
    val indices = codec.fields.map(_.index)
    assertEquals(indices, Vector(0, 1, 2, 3, 4))
  }
}

object FastCtorCacheSuite {
  // Test fixtures
  case class AllDefaults(a: Int = 1, b: String = "x", c: Boolean = true)
  case class PartialDefaults(
    i: Int = 42, d: Double, o: Option[String] = None,
    opt: Option[Int] = Some(7),
    optNull: Option[Int]
  )
  case class NoDefaults(x: Long, y: Boolean)
  case class GenericOpt[A](x: Option[A] = None)
  case class GenericOptInt(x: Option[Int] = None)
}
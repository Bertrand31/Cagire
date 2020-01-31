package utils

import scala.reflect.ClassTag
import cats.Monoid

object ArrayMonoid {

  implicit def arraySeq[T: ClassTag]: Monoid[Array[T]] = new Monoid[Array[T]] {

      override def empty: Array[T] = Array[T]()

      override def combine(x: Array[T], y: Array[T]): Array[T] = x ++ y
  }
}

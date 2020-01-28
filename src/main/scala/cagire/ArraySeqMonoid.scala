package data_structures

import scala.reflect.ClassTag
import scala.collection.immutable.ArraySeq
import cats.Monoid

object ArraySeqMonoid {

  implicit def arraySeq[T: ClassTag]: Monoid[ArraySeq[T]] = new Monoid[ArraySeq[T]] {

      override def empty: ArraySeq[T] = ArraySeq[T]()

      override def combine(x: ArraySeq[T], y: ArraySeq[T]): ArraySeq[T] = x ++ y
  }
}

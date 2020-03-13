package utils

import scala.util.chaining.scalaUtilChainingOps
import cats.Monoid
import org.roaringbitmap.RoaringBitmap

object RoaringBitmapMonoid {

  implicit def roaringBitmapMonoid: Monoid[RoaringBitmap] = new Monoid[RoaringBitmap] {

    override def empty: RoaringBitmap = new RoaringBitmap

    override def combine(x: RoaringBitmap, y: RoaringBitmap): RoaringBitmap = x.tap(_ or y)
  }
}

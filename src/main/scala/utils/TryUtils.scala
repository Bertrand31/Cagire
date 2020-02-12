package utils

import scala.util.Try

object TryUtils {

  implicit class EnhancedTry[A](t: Try[A]) {

    def tap(unaryFn: A => Unit): Try[A] =
      t.map(value => {
        unaryFn(value)
        value
      })
  }
}

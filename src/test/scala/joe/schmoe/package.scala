package joe

import scala.util.Random
import concurrent._
import concurrent.duration._

package object schmoe {
  def randomString(maxLen: Int = 10): String = {
    val minLen = 3
    val len = Random.nextInt(maxLen - minLen) + minLen
    val chars = new Array[Char](len)
    for (i <- 0 until len) {
      chars(i) = Random.nextPrintableChar()
    }
    new String(chars)
  }

  implicit class TestFuture[T](private val f: Future[T]) extends AnyVal {
    def await: T = Await.result(f, 10.seconds)
  }

}

package reductions

import scala.annotation._
import org.scalameter._
import common._

object ParallelParenthesesBalancingRunner {

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 40,
    Key.exec.maxWarmupRuns -> 80,
    Key.exec.benchRuns -> 120,
    Key.verbose -> true
  ) withWarmer(new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val length = 100000000
    val chars = new Array[Char](length)
    val threshold = 10000
    val seqtime = standardConfig measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime ms")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime ms")
    println(s"speedup: ${seqtime / fjtime}")
  }
}

object ParallelParenthesesBalancing {

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def balance(chars: Array[Char]): Boolean = {
    val length = chars.length

    @tailrec
    def innerBalance(i: Int, count: Int): Boolean = {
      if (i == length) count == 0
      else if (chars(i) == ')' && count == 0) false
      else {
        val newCount = chars(i) match {
          case '(' => 1
          case ')' => -1
          case _ => 0
        }
        innerBalance(i + 1, count + newCount)
      }
    }
    innerBalance(0, 0)
  }

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {

    require(threshold > 0)

    def traverse(idx: Int, until: Int, arg1: Int, arg2: Int): (Int, Int) = {
      if (idx >= until) (arg1, arg2)
      else {
        val (v1, v2) = chars(idx) match {
          case '(' => (arg1 + 1, if (arg2 == 0) +1 else arg2) // once the first parentheses has been found
          case ')' => (arg1 - 1, if (arg2 == 0) -1 else arg2) // the sign is set and it's never changed for this chunk
          case _ => (arg1, arg2)
        }
        traverse(idx + 1, until, v1, v2)
      }
    }

    def reduce(from: Int, until: Int): (Int, Int) = {
      if (until <= from || until - from <= threshold) {
        traverse(from, until, 0, 0)
      } else {
        val mid = from + (until - from) / 2
        val ((balanceLeft, signLeft), (balanceRight, signRight)) = parallel(
          reduce(from, mid),
          reduce(mid, until)
        )
        (balanceLeft + balanceRight, if (signLeft == 0) signRight else signLeft)
      }
    }

    val (balance, sign) = reduce(0, chars.length)
    balance == 0 && sign >= 0
  }

  // For those who want more:
  // Prove that your reduction operator is associative!

}

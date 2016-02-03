package com.hazelcast.Scala.dds

import scala.concurrent._
import collection.{ Map => cMap }

import com.hazelcast.Scala._

private object NumericDDS {
  def divOp[N: Numeric]: (N, N) => N = {
    implicitly[Numeric[N]] match {
      case f: Fractional[N] => f.div _
      case i: Integral[N] => i.quot _
      case n => (a: N, b: N) => n.fromInt(math.round(n.toFloat(a) / n.toFloat(b)))
    }
  }
  def numMedian[N: Numeric](a: N, b: N): N = {
      def num = implicitly[Numeric[N]]
    if (num.equiv(a, b)) a
    else {
      val divide = divOp
      divide(num.plus(a, b), num.fromInt(2))
    }
  }
}

trait NumericDDS[N] extends OrderingDDS[N] {
  implicit protected def num: Numeric[N]

  def sum()(implicit ec: ExecutionContext): Future[N] = this submit aggr.Sum()
  def product()(implicit ec: ExecutionContext): Future[N] = this submit aggr.Product()
  def mean()(implicit ec: ExecutionContext): Future[Option[N]] = submit(new aggr.Mean)

  def range()(implicit ec: ExecutionContext): Future[Option[N]] = {
    val n = num
    minMax() map { maybe =>
      maybe map {
        case (min, max) => n.minus(max, min)
      }
    }
  }

  def median()(implicit ec: ExecutionContext): Future[Option[N]] = {
    medianValues() map { maybe =>
      maybe map {
        case (a, b) => NumericDDS.numMedian(a, b)
      }
    }
  }

  def variance(nCorrection: (Int) => N = aggr.Variance.NoCorrection[N])(implicit ec: ExecutionContext): Future[Option[N]] =
    submit(aggr.Variance[N](nCorrection))

}

trait NumericGroupDDS[G, N] extends OrderingGroupDDS[G, N] {
  implicit protected def num: Numeric[N]

  def sum()(implicit ec: ExecutionContext): Future[cMap[G, N]] = this submit aggr.Sum()
  def product()(implicit ec: ExecutionContext): Future[cMap[G, N]] = this submit aggr.Product()
  def mean()(implicit ec: ExecutionContext): Future[cMap[G, N]] = submitGrouped(Aggregator.groupSome(new aggr.Mean))

  def range()(implicit ec: ExecutionContext): Future[cMap[G, N]] = {
    val n = num
    minMax() map { grouped =>
      grouped mapValues {
        case (min, max) => n.minus(max, min)
      }
    }
  }

  def median()(implicit ec: ExecutionContext): Future[cMap[G, N]] = {
    medianValues() map { grouped =>
      grouped mapValues {
        case (a, b) => NumericDDS.numMedian(a, b)
      }
    }
  }

  def variance(nCorrection: (Int) => N = aggr.Variance.NoCorrection[N])(implicit ec: ExecutionContext): Future[cMap[G, N]] =
    submitGrouped(Aggregator groupSome aggr.Variance[N](nCorrection))

}

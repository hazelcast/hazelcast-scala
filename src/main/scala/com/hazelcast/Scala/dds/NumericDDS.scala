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
  def mean()(implicit ec: ExecutionContext): Future[Option[N]] = submit(new aggr.Mean[N])

  def range()(implicit ec: ExecutionContext): Future[Option[N]] = {
    val n = num
    minMax().map(_.map {
      case (min, max) => n.minus(max, min)
    })(SameThread)
  }

  def median()(implicit ec: ExecutionContext): Future[Option[N]] = {
    medianValues().map(_.map {
      case (a, b) => NumericDDS.numMedian(a, b)
    })(SameThread)
  }

  def variance(nCorrection: (Int) => N = aggr.Variance.NoCorrection[N])(implicit ec: ExecutionContext): Future[Option[N]] =
    submit(aggr.Variance[N](nCorrection))

  def stdDev(nCorrection: (Int) => N = aggr.Variance.NoCorrection[N])(implicit ec: ExecutionContext, ev: N =:= Double): Future[Option[Double]] =
    variance(nCorrection).map(_.map(n => math.sqrt(n.asInstanceOf[Double])))(SameThread)

}

trait NumericGroupDDS[G, N] extends OrderingGroupDDS[G, N] {
  implicit protected def num: Numeric[N]

  def sum()(implicit ec: ExecutionContext): Future[cMap[G, N]] = this submit aggr.Sum()
  def product()(implicit ec: ExecutionContext): Future[cMap[G, N]] = this submit aggr.Product()
  def mean()(implicit ec: ExecutionContext): Future[cMap[G, N]] = submitGrouped(Aggregator.groupSome(new aggr.Mean))

  def range()(implicit ec: ExecutionContext): Future[cMap[G, N]] = {
    val n = num
    minMax().map(_.mapValues {
      case (min, max) => n.minus(max, min)
    }.toMap)(SameThread)
  }

  def median()(implicit ec: ExecutionContext): Future[cMap[G, N]] = {
    medianValues().map(_.mapValues {
      case (a, b) => NumericDDS.numMedian(a, b)
    }.toMap)(SameThread)
  }

  def variance(nCorrection: (Int) => N = aggr.Variance.NoCorrection[N])(implicit ec: ExecutionContext): Future[cMap[G, N]] =
    submitGrouped(Aggregator groupSome aggr.Variance[N](nCorrection))

  def stdDev(nCorrection: (Int) => N = aggr.Variance.NoCorrection[N])(implicit ec: ExecutionContext, ev: N =:= Double): Future[cMap[G, Double]] =
    variance(nCorrection).map(_.mapValues(n => math.sqrt(n.asInstanceOf[Double])).toMap)(SameThread)

}

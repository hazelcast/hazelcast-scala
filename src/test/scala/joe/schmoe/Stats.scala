package joe.schmoe

class Stats(
    private[this] var _sum: Long = 0,
    private[this] var _count: Int = 0,
    private[this] var _min: Int = Int.MaxValue,
    private[this] var _max: Int = Int.MinValue) {

  override def toString = s"Stats(sum=$sum, count=$count, min=$min, max=$max)"

  def update(num: Int): Stats = {
    _sum += num
    _count += 1
    _min = _min min num
    _max = _max max num
    this
  }
  def mergeWith(that: Stats): Stats = {
    _sum += that.sum
    _count += that.count
    _min = _min min that.min
    _max = _max max that.max
    this
  }

  def sum = _sum
  def count = _count
  def min = _min
  def max = _max
}

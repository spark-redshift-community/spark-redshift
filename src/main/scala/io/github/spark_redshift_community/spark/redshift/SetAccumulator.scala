package io.github.spark_redshift_community.spark.redshift

import org.apache.spark.util.AccumulatorV2

class SetAccumulator[T](var value: Set[T]) extends AccumulatorV2[T, Set[T]] {
  def this() = this(Set.empty[T])
  override def isZero: Boolean = value.isEmpty
  override def copy(): AccumulatorV2[T, Set[T]] = new SetAccumulator[T](value)
  override def reset(): Unit = value = Set.empty[T]
  override def add(v: T): Unit = value = value + v
  override def merge(other: AccumulatorV2[T, Set[T]]): Unit =
    value = value ++ other.value
}

package ignite.scala

import com.twitter.algebird.Semigroup
import java.io.Serializable

/**
 * Represents a reduction of Ignite's distributed closure results.
 * These are performed in the client and are akin to the join step in fork-join.
 */
sealed trait Reduction[T] extends Serializable {

  /**
   * Execute this reduction and return computed value.
   */
  def execute: Option[T]

  /**
   * Return a IgnitePipe representing the result of the reduction.
   *
   * Used for continuations.
   */
  def toPipe: IgnitePipe[T]
}

final case object EmptyReduction extends Reduction[Nothing] {

  override def execute = None

  override def toPipe = EmptyPipe
}

/**
 * Represents a reduction containing a computed value
 */
final case class ValueReduction[T](value: T)(implicit val compute: SIgniteCompute)
  extends Reduction[T] {

  override def execute = Some(value)

  override def toPipe = IgnitePipe.from(List(value))(compute)
}


object TransformValueReduction {
  def from[S, T](tvp: TransformValuePipe[S, T])(passedSg: Semigroup[T]): TransformValueReduction[S, T] =
    new TransformValueReduction[S, T] {
      override val compute = tvp.compute
      override val source = tvp.source
      override def transform = tvp.transform
      override def sg = passedSg
    }
}

/**
 * Represents a reduction performed by first performing
 * a transforming computation on the cluster followed by reduction.
 */
sealed abstract class TransformValueReduction[S, T] extends Reduction[T]
  with HasComputeConfig[S, T] {

  protected def sg: Semigroup[T]

  override def execute = compute.reduceOption(source)(transform)(sg)

  override def toPipe = ReduceHelper.toPipe[S, T](this)
}

object CacheAffinityValueReduction {
  def from[K, V, T](cap: CacheAffinityPipe[K, V, T])(passedSg: Semigroup[T]): CacheAffinityValueReduction[K, V, T] =
    new CacheAffinityValueReduction[K, V, T] {
      override val compute = cap.compute
      override val source = cap.source
      override def transform = cap.transform
      override def sg = passedSg
    }
}

/**
 * Represents a reduction performed by first performing
 * affinity (cache-collocated) computation followed by reduction.
 */
sealed abstract class CacheAffinityValueReduction[K, V, T] extends Reduction[T]
  with HasComputeConfig[CacheAffinity[K, V], T] {

  protected def sg: Semigroup[T]

  override def execute = compute.affinityReduceOption(source)(transform)(sg)

  override def toPipe = ReduceHelper.toPipe[CacheAffinity[K, V], T](this)
}

object FlatMapValueReduction {

  def from[S, T](fvp: FlatMapValuePipe[S, T])(passedSg: Semigroup[T]): FlatMapValueReduction[S, T] =
    new FlatMapValueReduction[S, T] {
      override val compute = fvp.compute
      override val source = fvp.source
      override def transform = fvp.transform
      override def sg = passedSg
    }
}

/**
 * Represents a reduction performed by first performing
 * a transforming computation on the cluster followed by
 * flattening and reduction (at the client).
 */
sealed abstract class FlatMapValueReduction[S, T] extends Reduction[T]
  with HasComputeConfig[S, TraversableOnce[T]] {

  protected def sg: Semigroup[T]

  def execute = compute
    .reduceOption(source)(transform.andThen(_.toList))(Semigroup.listSemigroup[T])
    .flatMap(_.reduceOption(sg.plus(_, _)))

  override def toPipe = ReduceHelper.toPipe[S, T](this)
}

object FlatMapCacheAffinityReduction {

  def from[K, V, T](fcap: FlatMapCacheAffinityPipe[K, V, T])(passedSg: Semigroup[T]): FlatMapCacheAffinityReduction[K, V, T] =
    new FlatMapCacheAffinityReduction[K, V, T] {
      override val compute = fcap.compute
      override val source = fcap.source
      override def transform = fcap.transform
      override def sg = passedSg
    }
}

/**
 * Represents a reduction performed by first performing
 * affinity (cache-collocated) computation followed by
 * flattening and reduction (at the client).
 */
sealed abstract class FlatMapCacheAffinityReduction[K, V, T] extends Reduction[T]
  with HasComputeConfig[CacheAffinity[K, V], TraversableOnce[T]] {

  protected def sg: Semigroup[T]

  def execute = compute
    .affinityReduceOption(source)(transform.andThen(_.toList))(Semigroup.listSemigroup[T])
    .flatMap(_.reduceOption(sg.plus(_, _)))

  override def toPipe = ReduceHelper.toPipe[CacheAffinity[K, V], T](this)
}

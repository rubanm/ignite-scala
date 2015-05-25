package ignite.scala

import com.twitter.algebird.Semigroup
import java.io.Serializable

object IgnitePipe {

  def empty: IgnitePipe[Nothing] = EmptyPipe

  def from[T](iter: Iterable[T])(implicit c: SIgniteCompute): IgnitePipe[T] =
    IterablePipe[T](iter)

  def from[K, V, T](iter: Iterable[CacheAffinity[K, V]])(f: CacheAffinity[K, V] => T)(implicit c: SIgniteCompute): CacheAffinityPipe[K, V, T] =
    new CacheAffinityPipe[K, V, T] {
      override def compute = c
      override def source = iter
      override def transform = f
    }
}

/**
 * Provides composable distributed closures that can run on Apache Ignite.
 */
sealed trait IgnitePipe[T] extends Serializable {
  // TODO: make this covariant

  /** Transform each element using the function f */
  def map[U](f: T => U): IgnitePipe[U]

  /**
   * Transform each value using the function f and flatten the result.
   *
   * Flattening step is performed on the client. If you have a chain of flatMaps,
   * all functions in the chain are composed and flattening is performed once
   * on the client.
   *
   * To manually split the flatMap chain, use .fork. This is useful when
   * dealing with long, lazy chains, or when adding a barrier is desired
   * so that the computation can be re-used.
   */
  def flatMap[U](f: T => TraversableOnce[U]): IgnitePipe[U]

  /**
   * Filter elements using the function f.
   *
   * Implemented as a flatMap executed on the client.
   */
  def filter(f: T => Boolean): IgnitePipe[T] =
    flatMap { t => if (f(t)) Iterator(t) else Iterator.empty }

  /**
   * Prepare a Reduction based on the provided Semigroup.
   *
   * Note that results can arrived from cluster nodes in any order,
   * so the operation has to be associative and commutative.
   */
  def reduce()(implicit sg: Semigroup[T]): Reduction[T]

  /**
   * Manually add a fork in the execution chain.
   * This creates a barrier, which means the subsequent transforms
   * are planned on a fresh Ignite closure.
   */
  def fork: IgnitePipe[T]

  /** Execute the chain and return the computed values. */
  def execute: Iterable[T]

  /** Execute the chain and create a new pipe from the result. */
  def executeAndThen[U](f: Iterable[T] => IgnitePipe[U]): IgnitePipe[U] =
    f(execute)
}

final case object EmptyPipe extends IgnitePipe[Nothing] {

  override def map[U](f: Nothing => U) = sys.error("map called on EmptyPipe")

  override def flatMap[U](f: Nothing => TraversableOnce[U]) =
    sys.error("flatMap called on EmptyPipe")

  override def reduce()(implicit sg: Semigroup[Nothing]) = EmptyReduction

  override def fork = this

  override def execute = Iterable.empty[Nothing]
}

/**
 * Trait for pipes that hold information about
 * the cluster along with the source and transform
 * for underlying computation.
 */
trait HasComputeConfig[S, T] {
  def compute: SIgniteCompute

  def source: Iterable[S]

  def transform: S => T
}

/**
 * Represents a transforming computation on the cluster.
 */
sealed abstract class TransformValuePipe[S, T] extends IgnitePipe[T]
  with HasComputeConfig[S, T] {

  override def map[U](f: T => U) = PipeHelper.toTransformValuePipe[S, T, U](this)(f)

  def flatMap[U](f: T => TraversableOnce[U]) =
    PipeHelper.toFlatMapValuePipe[S, T, U](this)(f)

  override def reduce()(implicit sg: Semigroup[T]) =
    TransformValueReduction.from(this)(sg)

  override def fork = flatMap(Iterable(_)).map(identity)

  override def execute = compute.apply(source)(transform)
}

/**
 * Represents a transforming computation on the cluster
 * followed by flattening of results done at the client.
 */
sealed abstract class FlatMapValuePipe[S, T] extends IgnitePipe[T]
  with HasComputeConfig[S, TraversableOnce[T]] {

  override def map[U](f: T => U) =
    PipeHelper.toTransformValuePipe[S, T, U](this)(f)

  override def flatMap[U](f: T => TraversableOnce[U]) =
    PipeHelper.toFlatMapValuePipe[S, T, U](this)(f)

  override def reduce()(implicit sg: Semigroup[T]) =
    FlatMapValueReduction.from(this)(sg)

  override def fork = flatMap(Iterable(_)).map(identity)

  override def execute = compute.flatMapApply[S, T](source)(transform)
}

/**
 * Represents a transforming affinity (cache-collocation)
 * computation on the cluster.
 */
sealed abstract class CacheAffinityPipe[K, V, T] extends IgnitePipe[T]
  with HasComputeConfig[CacheAffinity[K, V], T] {

  override def map[U](f: T => U) =
    PipeHelper.toCacheAffinityPipe[K, V, T, U](this)(f)

  override def flatMap[U](f: T => TraversableOnce[U]) =
    PipeHelper.toFlatMapCacheAffinityPipe[K, V, T, U](this)(f)

  override def reduce()(implicit sg: Semigroup[T]) =
    CacheAffinityValueReduction.from(this)(sg)

  override def fork = flatMap(Iterable(_)).map(identity)

  override def execute = compute.affinityApply(source)(transform)
}

/**
 * Represents a transforming affnity (cache-collocated)
 * computation on the cluster followed by flattening of results
 * done at the client.
 */
sealed abstract class FlatMapCacheAffinityPipe[K, V, T] extends IgnitePipe[T]
  with HasComputeConfig[CacheAffinity[K, V], TraversableOnce[T]] {

  override def map[U](f: T => U) =
    PipeHelper.toCacheAffinityPipe[K, V, T, U](this)(f)

  override def flatMap[U](f: T => TraversableOnce[U]) =
    PipeHelper.toFlatMapCacheAffinityPipe[K, V, T, U](this)(f)

  override def reduce()(implicit sg: Semigroup[T]) =
    FlatMapCacheAffinityReduction.from(this)(sg)

  override def fork = flatMap(Iterable(_)).map(identity)

  override def execute = compute.flatMapAffinityApply(source)(transform)
}

/**
 * A pipe containing a sequence of values.
 * Can be generally used as the starting point in the execution chain.
 */
final case class IterablePipe[T](iter: Iterable[T])(implicit val compute: SIgniteCompute)
  extends IgnitePipe[T] {

  override def map[U](f: T => U) = PipeHelper.toTransformValuePipe[T, U](this)(f)

  def flatMap[U](f: T => TraversableOnce[U]) =
    PipeHelper.toFlatMapValuePipe[T, U](this)(f)

  override def reduce()(implicit sg: Semigroup[T]) =
    ValueReduction(iter.reduce(sg.plus(_, _)))(compute)

  override def fork = this

  override def execute = iter
}

/**
 * Helpers for switching betweeen IgnitePipe types.
 */
private object PipeHelper {

  def toTransformValuePipe[T, U](ip: IterablePipe[T])(f: T => U): TransformValuePipe[T, U] =
    new TransformValuePipe[T, U] {
      override val compute = ip.compute
      override val source = ip.iter
      override def transform = f
    }

  def toTransformValuePipe[S, T, U](tvp: TransformValuePipe[S, T])(f: T => U): TransformValuePipe[S, U] =
    new TransformValuePipe[S, U] {
      override val compute = tvp.compute
      override val source = tvp.source
      override def transform = tvp.transform.andThen(f)
    }

  def toFlatMapValuePipe[S, T, U](tvp: TransformValuePipe[S, T])(f: T => TraversableOnce[U]): FlatMapValuePipe[S, U] =
    new FlatMapValuePipe[S, U] {
      override val compute = tvp.compute
      override val source = tvp.source
      override def transform = tvp.transform.andThen(f)
    }

  def toFlatMapValuePipe[S, T, U](fvp: FlatMapValuePipe[S, T])(f: T => TraversableOnce[U]): FlatMapValuePipe[S, U] =
    new FlatMapValuePipe[S, U] {
      override val compute = fvp.compute
      override val source = fvp.source
      override def transform = fvp.transform.andThen(_.map(f)).andThen(_.flatten)
    }

  def toFlatMapValuePipe[T, U](ip: IterablePipe[T])(f: T => TraversableOnce[U]): FlatMapValuePipe[T, U] =
    new FlatMapValuePipe[T, U] {
      override val compute = ip.compute
      override val source = ip.iter
      override def transform = f
    }

  def toTransformValuePipe[S, T, U](fvp: FlatMapValuePipe[S, T])(f: T => U): TransformValuePipe[T, U] =
    IterablePipe(fvp.execute)(fvp.compute).map(f)

  def toCacheAffinityPipe[K, V, T, U](cap: CacheAffinityPipe[K, V, T])(f: T => U): CacheAffinityPipe[K, V, U] =
    new CacheAffinityPipe[K, V, U] {
      override val compute = cap.compute
      override val source = cap.source
      override def transform = cap.transform.andThen(f)
    }

  def toCacheAffinityPipe[K, V, T, U](fcap: FlatMapCacheAffinityPipe[K, V, T])(f: T => U): TransformValuePipe[T, U] =
    IterablePipe(fcap.execute)(fcap.compute).map(f)

  def toFlatMapCacheAffinityPipe[K, V, T, U](cap: CacheAffinityPipe[K, V, T])(f: T => TraversableOnce[U]): FlatMapCacheAffinityPipe[K, V, U] =
    new FlatMapCacheAffinityPipe[K, V, U] {
      override val compute = cap.compute
      override val source = cap.source
      override def transform = cap.transform.andThen(f)
    }

  def toFlatMapCacheAffinityPipe[K, V, T, U](fcap: FlatMapCacheAffinityPipe[K, V, T])(f: T => TraversableOnce[U]): FlatMapCacheAffinityPipe[K, V, U] =
    new FlatMapCacheAffinityPipe[K, V, U] {
      override val compute = fcap.compute
      override val source = fcap.source
      override def transform = fcap.transform.andThen(_.map(f)).andThen(_.flatten)
    }
}

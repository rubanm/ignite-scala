package ignite.scala

import com.twitter.algebird.Semigroup
import org.apache.ignite.IgniteCache
import java.io.Serializable

object IgnitePipe {

  def empty: IgnitePipe[Nothing] = EmptyPipe

  def from[T](iter: Iterable[T])(implicit c: ComputeRunner): IgnitePipe[T] =
    IterablePipe[T](iter)

  def from[T](iterGen: () => Iterable[T])(implicit c: ComputeRunner): IgnitePipe[T] =
    from(List(())).flatMap(_ => iterGen())

  def collocated[K, V, T](cache: IgniteCache[K, V], keys: Set[K])(f: (IgniteCache[K, V], K) => T)(implicit c: ComputeRunner): CacheAffinityPipe[K, V, T] =
    new CacheAffinityPipe[K, V, T] {
      override def compute = c
      override def source = keys.map(CacheAffinity[K, V](cache.getName, _))
      override def transform = { ca: CacheAffinity[K, V] => f(cache, ca.key) }
      // TODO: this can be inefficient. keyset enrichment should happen in ComputeRunner
    }
}

/**
 * Provides composable distributed closures that can run on Apache Ignite.
 *
 * Allows chaining functions to be executed on the cluster. Reduction is done
 * on the client. Note that pipe operations like flattening, filtering are also
 * performed on the client after gathering results from the nodes.
 *
 * Best practice is to push computations to the cluster as much as possible
 * and flatten, filter on the client only if the scatter-gather overhead is
 * acceptable and results can fit on the client.
 */
sealed trait IgnitePipe[T] extends Serializable {
  // TODO: make this covariant

  /**
   * Transform each element using the function f.
   *
   * This is executed on the cluster nodes. Chained map transforms
   * are composed and executed once on the cluster nodes. Use .fork
   * to manually split the chain if tuning is required.
   */
  def map[U](f: T => U): IgnitePipe[U]

  /**
   * Transform each value using the function f and flatten the result.
   *
   * Note: This is not a monadic composition.
   *
   * Flatten step is performed on the client. If you have a chain of flatMaps,
   * all functions in the chain are composed and flattening is performed once
   * on the client.
   *
   * To manually split the flatMap chain, use .fork. Forking is useful when
   * dealing with long, lazy chains, or when adding a barrier is desired.
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
  def reduce(implicit sg: Semigroup[T]): Reduction[T]

  /** Merge two pipes of the same type*/
  def ++(p: IgnitePipe[T]): IgnitePipe[T] = p match {
    case IterablePipe(iter) if iter.isEmpty => this
    case _ => MergedPipe(this, p)
  }

  /**
   * Manually add a fork in the execution chain.
   * This creates a barrier, which means the subsequent transforms
   * are planned on a fresh Ignite closure.
   */
  def fork: IgnitePipe[T]

  /** Execute the chain and return the computed values. */
  def execute: Iterable[T]
}

final case object EmptyPipe extends IgnitePipe[Nothing] {

  override def map[U](f: Nothing => U) = sys.error("map called on EmptyPipe")

  override def flatMap[U](f: Nothing => TraversableOnce[U]) =
    sys.error("flatMap called on EmptyPipe")

  override def reduce(implicit sg: Semigroup[Nothing]) = EmptyReduction

  override def fork = this

  override def execute = Iterable.empty[Nothing]
}

/**
 * Trait for pipes that hold information about
 * the cluster along with the source and transform
 * for underlying computation.
 */
trait HasComputeConfig[S, T] {
  def compute: ComputeRunner

  def source: Iterable[S]

  def transform: S => T
}

/**
 * Represents a transforming computation on the cluster.
 */
sealed abstract class TransformValuePipe[S, T] extends IgnitePipe[T]
  with HasComputeConfig[S, T] {

  override def map[U](f: T => U) = PipeHelper.toTransformValuePipe[S, T, U](this)(f)

  override def flatMap[U](f: T => TraversableOnce[U]) =
    PipeHelper.toFlatMapValuePipe[S, T, U](this)(f)

  override def reduce(implicit sg: Semigroup[T]) =
    TransformValueReduction.from(this)(sg)

  override def fork = PipeHelper.forkPipe(this)

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

  override def reduce(implicit sg: Semigroup[T]) =
    FlatMapValueReduction.from(this)(sg)

  override def fork = PipeHelper.forkPipe(this)

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

  override def reduce(implicit sg: Semigroup[T]) =
    CacheAffinityValueReduction.from(this)(sg)

  override def fork = PipeHelper.forkPipe(this)

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

  override def reduce(implicit sg: Semigroup[T]) =
    FlatMapCacheAffinityReduction.from(this)(sg)

  override def fork = PipeHelper.forkPipe(this)

  override def execute = compute.flatMapAffinityApply(source)(transform)
}

final case class MergedPipe[T](left: IgnitePipe[T], right: IgnitePipe[T])
  extends IgnitePipe[T] {

  override def map[U](f: T => U) =
    MergedPipe(left.map(f), right.map(f))

  override def flatMap[U](f: T => TraversableOnce[U]) =
    MergedPipe(left.flatMap(f), right.flatMap(f))

  override def reduce(implicit sg: Semigroup[T]) =
    MergedReduction(left.reduce, right.reduce)

  override def fork = this

  override def execute = left.execute ++ right.execute
}

/**
 * A pipe containing a sequence of values.
 *
 * Can be generally used as the starting point in the execution chain. The sequence is
 * partitioned and load balanced across the cluster nodes.
 */
final case class IterablePipe[T](iter: Iterable[T])(implicit val compute: ComputeRunner)
  extends IgnitePipe[T] {

  override def map[U](f: T => U) = PipeHelper.toTransformValuePipe[T, U](this)(f)

  override def flatMap[U](f: T => TraversableOnce[U]) =
    PipeHelper.toFlatMapValuePipe[T, U](this)(f)

  override def reduce(implicit sg: Semigroup[T]) =
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

  // this adds a barrier. the supplied function f is executed
  // on the cluster only after flatten step of the input pipe is
  // executed on the client
  def toTransformValuePipe[S, T, U](fvp: FlatMapValuePipe[S, T])(f: T => U): TransformValuePipe[T, U] =
    IterablePipe(fvp.execute)(fvp.compute).map(f)

  def toCacheAffinityPipe[K, V, T, U](cap: CacheAffinityPipe[K, V, T])(f: T => U): CacheAffinityPipe[K, V, U] =
    new CacheAffinityPipe[K, V, U] {
      override val compute = cap.compute
      override val source = cap.source
      override def transform = cap.transform.andThen(f)
    }

  // this adds a barrier similar to the non-affinity version
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

  def forkPipe[S, T](tvp: TransformValuePipe[S, T]): IgnitePipe[T] =
    IgnitePipe.from(() => tvp.execute)(tvp.compute)

  def forkPipe[S, T](fvp: FlatMapValuePipe[S, T]): IgnitePipe[T] =
    IgnitePipe.from(() => fvp.execute)(fvp.compute)

  def forkPipe[K, V, T](cap: CacheAffinityPipe[K, V, T]): IgnitePipe[T] =
    IgnitePipe.from(() => cap.execute)(cap.compute)

  def forkPipe[K, V, T](fcap: FlatMapCacheAffinityPipe[K, V, T]): IgnitePipe[T] =
    IgnitePipe.from(() => fcap.execute)(fcap.compute)
}

object ReduceHelper {
  // creates a pipe representing the result of the reduction
  def toPipe[S, T](r: Reduction[T] with HasComputeConfig[S, _]): IgnitePipe[T] =
    new TransformValuePipe[T, T] {
      override val compute = r.compute
      override def source = r.execute.toIterable
      override def transform = identity
    }
}

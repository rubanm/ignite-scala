package ignite.scala

import com.twitter.algebird.Semigroup
import com.twitter.logging.Logger
import org.apache.ignite._
import org.apache.ignite.lang._
import scala.collection.JavaConverters._

/**
 * Basic accumulating version of IgniteReducer.
 *
 * Values are summed in a local var as they are received
 * from closure computations on the cluster as defined by
 * the supplied Semigroup.
 */
class AccumulatingReducer[A](sg: Semigroup[A], n: Int)
    extends IgniteReducer[A, A] {

  require(n > 0)

  private[this] val log = Logger.get(getClass)
  private[this] object Lock

  private[this] var collected = Option.empty[A]
  private[this] var count = 0

  override def collect(a: A): Boolean = Lock.synchronized {
    log.debug(s"Collecting value $a")
    if (count == n)
      sys.error(s"Buffer overflow. Already reached size $count")
    else {
      collected match {
        case None => collected = Some(a)
        case Some(b) => collected = Some(sg.plus(b, a))
      }
      count = count + 1
      if (count == n) false
      else true
    }
  }

  override def reduce: A = collected match {
    case None => sys.error("No values collected by reducer")
    case Some(c) => c
  }
}

/**
 * Conversions from Scala function to Ignite's closure classes.
 */
private object IgniteClosureConversions {

  def scala2closure[A, B](f: Function1[A, B]): IgniteClosure[A, B] =
    new IgniteClosure[A, B]() { override def apply(a: A) = f(a) }

  def scala2runnable[A](f: () => Unit): IgniteRunnable =
    new IgniteRunnable() { override def run = f() }

  def scala2callable[A](f: () => A): IgniteCallable[A] =
    new IgniteCallable[A]() { override def call = f() }

  def scala2reducer[A](sg: Semigroup[A], n: Int): IgniteReducer[A, A] =
    new AccumulatingReducer[A](sg, n)
}

final case class CacheAffinity[K, V](cacheName: String, key: K)

/**
 * Provides Scala-friendly api to IgniteCompute class.
 */
final case class ComputeRunner(ic: IgniteCompute) {
  import IgniteClosureConversions._

  def apply[A, B](x: A)(f: A => B): B =
    ic.apply[B, A](scala2closure(f), x)

  def apply[A, B](xs: Iterable[A])(f: A => B): Iterable[B] =
    xs match {
      case Nil => Nil // ignite does not handle empty iterables correctly
      case iter => ic.apply[A, B](scala2closure(f), iter.asJavaCollection).asScala
    }

  def flatMapApply[A, B](xs: Iterable[A])(f: A => TraversableOnce[B]): Iterable[B] =
    apply[A, TraversableOnce[B]](xs)(f).flatten

  def affinityApply[A, K, V](cas: Iterable[CacheAffinity[K, V]])(f: CacheAffinity[K, V] => A): Iterable[A] = cas
    .map { ca =>
      List(ic.affinityCall(ca.cacheName, ca.key,
        scala2callable(() => f(ca))))
    }
    .reduceOption(Semigroup.listSemigroup[A].plus(_, _))
    .toIterable.flatten

  def flatMapAffinityApply[A, K, V](cas: Iterable[CacheAffinity[K, V]])(f: CacheAffinity[K, V] => TraversableOnce[A]): Iterable[A] =
    affinityApply[TraversableOnce[A], K, V](cas)(f).flatten

  def affinityReduceOption[A, K, V](cas: Iterable[CacheAffinity[K, V]])(f: CacheAffinity[K, V] => A)(sg: Semigroup[A]): Option[A] =
    affinityApply[A, K, V](cas)(f).reduceOption(sg.plus(_, _))

  // TODO: size is O(n) for Iterables
  def reduceOption[A, B](xs: Iterable[A])(m: A => B)(sg: Semigroup[B]): Option[B] =
    xs match {
      case Nil => None
      case iter => Some(
        ic.apply[B, B, A](scala2closure(m), iter.toList.asJavaCollection,
          scala2reducer(sg, iter.size)))
    }

  //
  // the following are currently unused
  //

  def broadcastFn[A, B](x: A)(f: A => B): Iterable[B] =
    ic.broadcast[B, A](scala2closure(f), x).asScala

  def broadcastRun(x: () => Unit): Unit =
    ic.broadcast(scala2runnable(x))

  def broadcastCall[A](x: () => A): Iterable[A] =
    ic.broadcast[A](scala2callable(x)).asScala

  def call[A](x: () => A): A =
    ic.call[A](scala2callable(x))

  def call[A](xs: Iterable[() => A]): Iterable[A] =
    xs match {
      case Nil => Nil
      case iter => ic.call[A](iter.map(scala2callable(_)).asJavaCollection).asScala
    }

  def reduceOptionCall[A](xs: Iterable[() => A])(sg: Semigroup[A]): Option[A] =
    xs match {
      case Nil => None
      case iter => Some(
        ic.call[A, A](iter.map(scala2callable(_)).toList.asJavaCollection,
          scala2reducer(sg, iter.size)))
    }
}

# ignite scala

Scala API for distributed closures on [Apache Ignite](https://ignite.incubator.apache.org/). Inspired by [Scalding](https://github.com/twitter/scalding/).

http://apacheignite.readme.io/v1.0/docs/distributed-closures

#### example 0 - cluster setup
```scala
import org.apache.ignite._
import org.apache.ignite.configuration._
import ignite.scala._

val cfg = new IgniteConfiguration
// configure as appropriate

val ignite = Ignition.start(cfg)
val compute = ignite.compute(cluster)
implicit val cr = ComputeRunner(compute)
```
#### example 1 - character count
```scala
import com.twitter.algebird.Semigroup

implicit val sg = Semigroup.intSemigroup

val chain = IgnitePipe.from("The quick brown fox jumps over the lazy dog.".split(" "))
  .map(_.length) // fork
  .reduce // join

chain.execute // Option[Int]
```
#### example 2 - working with distributed cache
```scala
val cache = {
  val cfg = new CacheConfiguration[K, V]
  cfg.setName(name)
  ignite.getOrCreateCache(cfg)
}

val process: V => R
val isValid: R => Boolean

IgnitePipe.from(keys)
  .map { k =>
    val v = cache.get(k)
    process(v)
  }
  .filter(isValid)
  .execute // Iterable[R]
```
#### api

```scala
trait IgnitePipe[T] {
  
  def map[U](f: T => U): IgnitePipe[U]
  
  def flatMap[U](f: T => TraversableOnce[U]): IgnitePipe[U]
  
  def ++(p: IgnitePipe[T]): IgnitePipe[T]
  
  def reduce(implicit sg: Semigroup[T]): Reduction[T]
  
  def execute: Iterable[T]
}

trait Reduction[T] {

  def execute: Option[T]

  def toPipe: IgnitePipe[T]
}
```

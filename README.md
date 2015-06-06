# ignite scala

Scala API for distributed closures on [Apache Ignite](https://ignite.incubator.apache.org/). Inspired by [Scalding](https://github.com/twitter/scalding/).

http://apacheignite.readme.io/v1.0/docs/distributed-closures

#### example 0 - cluster setup
```scala
import org.apache.ignite._
import org.apache.ignite.configuration._
import ignite.scala._

val cfg = new IgniteConfiguration // configure as appropriate
val ignite = Ignition.start(cfg)
val compute = ignite.compute(ignite.cluster)
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

val process: V => R // computation
val isValid: R => Boolean

IgnitePipe.from(keys)
  .map { k =>
    val v = cache.get(k)
    process(v)
  }
  .filter(isValid)
  .execute // Iterable[R]
```
#### example 3 - collocating compute with cache
Ignite allows routing computations to the nodes where data is cached.
```scala
IgnitePipe.collocated(cache, keys) { (c, k) =>
  val v = c.localPeek(k)
  process(v)
}
.filter(isValid)
.execute
```
#### example 4 - more chaining
```scala
val cache: IgniteCache[K, V]
val db: CacheJdbcBlobStore[K, V]

val cacheResults = IgnitePipe.from(keys)
  .map { k => cacheGetAndCompute(cache, k) }

val dbResults = IgnitePipe.from(keys)
  .map { k => dbGetAndCompute(db, k) }
  
val combined = (cacheResults ++ dbResults)
  .reduce // reduction could be to consolidate cache and db for instance
  .toPipe // continuation
  
combined.map { exportResults(_) }.execute
```

#### installing

Add the following to your build.sbt (fetches from sonatype)
```scala
resolvers += Resolver.sonatypeRepo("releases")
libraryDependencies += "com.github.rubanm" %% "ignite-scala" % "0.0.1"
```
#### core api

```scala
/* Provides composable distributed closures that can run on Apache Ignite. */
trait IgnitePipe[T] {
  
  def map[U](f: T => U): IgnitePipe[U]
  
  def flatMap[U](f: T => TraversableOnce[U]): IgnitePipe[U]
  
  def ++(p: IgnitePipe[T]): IgnitePipe[T]
  
  def reduce(implicit sg: Semigroup[T]): Reduction[T]
  
  def execute: Iterable[T]
}

/* Represents a reduction of the distributed closure results.*/
trait Reduction[T] {

  def execute: Option[T]

  def toPipe: IgnitePipe[T]
}
```

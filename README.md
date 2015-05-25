# ignite scala

Scala API for distributed closures on [Apache Ignite](https://ignite.incubator.apache.org/). Inspired by [Scalding](https://github.com/twitter/scalding/).

http://apacheignite.readme.io/v1.0/docs/distributed-closures

#### Example 0 - Cluster setup
```scala
import org.apache.ignite._
import org.apache.ignite.configuration._
import ignite.scala._

val cfg = new IgniteConfiguration
// configure as appropriate

val ignite = Ignition.start(cfg)
val compute = ignite.compute(cluster)
implicit val ic = SIgniteCompute(compute)
```
#### Example 1 - Character count
```scala
import com.twitter.algebird.Semigroup

implicit val sg = Semigroup.intSemigroup

val chain = IgnitePipe.from("The quick brown fox jumps over the lazy dog.".split(" "))
  .map(_.length) // fork
  .reduce // join

chain.execute
```
More examples to follow.

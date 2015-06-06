package ignite.scala

import com.twitter.algebird.Semigroup
import org.apache.ignite._
import org.apache.ignite.cache.affinity._
import org.apache.ignite.configuration._
import org.apache.ignite.marshaller.optimized.OptimizedMarshaller
import org.scalatest.{ FunSpec, BeforeAndAfterAll }
import ignite.scala._

class ClusterTests extends FunSpec with BeforeAndAfterAll {

  val marsh = new OptimizedMarshaller
  marsh.setRequireSerializable(false) // for closures with non serializable objects
  val cfg = new IgniteConfiguration
  cfg.setMarshaller(marsh)
  cfg.setPeerClassLoadingEnabled(true)

  val ignite = Ignition.start(cfg)
  val cluster = ignite.cluster
  val compute = ignite.compute(cluster)

  implicit val cr = ComputeRunner(compute)

  override def afterAll() {
    ignite.close()
  }

  describe("character count test") {
    implicit val sg = Semigroup.intSemigroup

    it ("should execute") {

      val count = IgnitePipe.from("apple pie".split(" "))
        .map(_.length)
        .reduce
        .execute

      assert(Some(8) == count)
    }

    it ("should execute merge") {
      val pipe1 = IgnitePipe.from(Seq(1, 2, 3)).map(identity)
      val pipe2 = IgnitePipe.from(Seq(4, 5, 6)).map(identity)
      // .map here forces the closure execution to cluster nodes

      val sum = (pipe1 ++ pipe2)
        .reduce
        .execute

      assert(Some(21) == sum)
    }
  }

  describe("cache put and get test") {
    val cachecfg = new CacheConfiguration[Int, Seq[String]]
    cachecfg.setName("cache-test")

    it ("should execute") {
      val cache = ignite.getOrCreateCache(cachecfg)
      cache.put(1, Seq("one", "ek"))
      cache.put(2, Seq("two", "do"))
      cache.put(3, Seq("three", "teen"))

      val data = IgnitePipe.from(Seq(1, 2, 3))
        .map { k =>
          cache.get(k)
        }
        .flatMap(identity)
        .execute

      assert(Set("one", "two", "three", "ek", "do", "teen") == data.toSet)
    }

    it ("should execute collocated") {
      // re-uses existing cache
      val cache = ignite.getOrCreateCache(cachecfg)
      val data = IgnitePipe
        .collocated(cache, Set(1, 2, 3)) { (c, k) =>
          c.localPeek(k)
        }
        .flatMap(identity)
        .execute

      assert(Set("one", "two", "three", "ek", "do", "teen") == data.toSet)
    }
  }
}

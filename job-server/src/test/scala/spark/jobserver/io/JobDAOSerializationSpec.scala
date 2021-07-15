package spark.jobserver.io

import org.scalatest.{FunSpecLike, Matchers}

class JobDAOSerializationSpec extends FunSpecLike with Matchers {

  describe("Test serialization and deserialization for job results") {
    it("should serialize and deserialize a string") {
      val someValue = "Something"
      val serialized = JobDAOActor.serialize(someValue)
      JobDAOActor.deserialize(serialized) should equal(someValue)
    }

    it("should serialize and deserialize an integer") {
      val someValue = 4
      val serialized = JobDAOActor.serialize(someValue)
      JobDAOActor.deserialize(serialized) should equal(someValue)
    }

    it("should serialize and deserialize array of bytes") {
      val someValue = "Something".map(_.toByte).toArray
      val serialized = JobDAOActor.serialize(someValue)
      JobDAOActor.deserialize(serialized) should equal(someValue)
    }

    it("should serialize and deserialize a list") {
      val someValue = List(1, 2, 3, 4, 5)
      val serialized = JobDAOActor.serialize(someValue)
      JobDAOActor.deserialize(serialized) should equal(someValue)
    }

    it("should serialize and deserialize a map") {
      val someValue = Map("A" -> 1, "B" -> 2, "C" -> 3)
      val serialized = JobDAOActor.serialize(someValue)
      JobDAOActor.deserialize(serialized) should equal(someValue)
    }

    it("should serialize and deserialize a map which contains another map") {
      val someValue = Map("A" -> 1, "B" -> 2, "C" -> 3, "D" -> Map("A" -> 1))
      val serialized = JobDAOActor.serialize(someValue)
      JobDAOActor.deserialize(serialized) should equal(someValue)
    }

    it("should serialize and deserialize a nested map") {
      val someValue = Map("second" -> Map("K" -> Map("one" -> 1)))
      val serialized = JobDAOActor.serialize(someValue)
      JobDAOActor.deserialize(serialized) should equal(someValue)
    }

    it("should serialize and deserialize a map which contains sequences") {
      val someValue = Map("first" -> Seq(1, 2, Seq("a", "b")))
      val serialized = JobDAOActor.serialize(someValue)
      JobDAOActor.deserialize(serialized) should equal(someValue)
    }
  }
}

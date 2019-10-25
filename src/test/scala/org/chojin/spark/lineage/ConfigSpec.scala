package org.chojin.spark.lineage

import org.chojin.spark.lineage.reporter.{DynamodbReporter, KinesisReporter, Reporter}
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.{WordSpec, Matchers => ScalaTestMatchers}

class ConfigSpec extends WordSpec with MockitoSugar with ScalaTestMatchers with ArgumentMatchersSugar {

  "ConfigSpec" should {

    "createInstancesOf" in {
      val reporters: List[Reporter] = Config.createInstancesOf("reporter")

      reporters shouldEqual List(
        DynamodbReporter("my-table", Some("us-west-2"), None),
        KinesisReporter("my-stream", Some("us-west-2"), "0", Some("gzip"))
      )
    }
  }
}

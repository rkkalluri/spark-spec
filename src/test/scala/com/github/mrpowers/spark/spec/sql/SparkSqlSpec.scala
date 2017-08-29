package com.github.mrpowers.spark.spec.sql

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import com.github.mrpowers.spark.models.PersonWithAge
import com.github.mrpowers.spark.spec.SparkSessionTestWrapper
import org.apache.spark.sql.Dataset
import org.scalatest.FunSpec


/**
  * Created by rkalluri on 8/29/17.
  */
class SparkSqlSpec extends FunSpec with SparkSessionTestWrapper with DatasetComparer {

  import spark.implicits._

  it("#createTempView") {

    val sourceDS: Dataset[PersonWithAge] = Seq(
      PersonWithAge("Alice", 12),
      PersonWithAge("Bob", 42),
      PersonWithAge("Cody", 10),
      PersonWithAge("Dane", 50)
    ).toDS

    sourceDS.createTempView("sourceDS_VIEW")

    val actualDS = spark.sql("select name ,age from sourceDS_VIEW  where age > 20")
                         .as[PersonWithAge]

    val expectedDS: Dataset[PersonWithAge] = Seq(
      PersonWithAge("Bob", 42),
      PersonWithAge("Dane", 50)
    ).toDS

    assertSmallDatasetEquality(actualDS, expectedDS)

  }
}

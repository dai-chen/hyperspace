package org.opensearch.maximus.command.mv

import org.apache.spark.SparkFunSuite

import com.microsoft.hyperspace.SparkInvolvedSuite

class MaximusCreateMVCommandTest extends SparkFunSuite with SparkInvolvedSuite {

  val createMvCmd = MaximusCreateMVCommand(Option.empty,
    "testMv", "SELECT name, AVG(age) FROM t001 GROUP BY name")

  override def beforeAll(): Unit = {
    // spark.sql("CREATE DATABASE temp LOCATION '/tmp/test1.db' ")
    // spark.sql("CREATE TABLE temp.t001 (name STRING, age INT)")
    spark.createDataFrame(Seq(("a", 1)))
      .toDF(Seq("name", "age"): _*)
      .createOrReplaceTempView("t001")
  }

  test("Create MV") {
    createMvCmd.run(spark)
  }
}

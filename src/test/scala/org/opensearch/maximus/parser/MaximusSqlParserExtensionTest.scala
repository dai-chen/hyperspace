package org.opensearch.maximus.parser

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.internal.SQLConf

import com.microsoft.hyperspace.SparkInvolvedSuite

class MaximusSqlParserExtensionTest extends SparkFunSuite with SparkInvolvedSuite {

  val maximusSqlParserExtension = new MaximusSqlParserExtension(new SQLConf)

  test("Fall back to Spark parser for non-Maximus grammar") {
    val plan = maximusSqlParserExtension.parsePlan("SELECT * FROM alb_logs_table")
    assert(plan != null)
  }
}

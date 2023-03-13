package org.opensearch.maximus.parser

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.internal.SQLConf

class MaximusSqlParserExtension(
    conf: SQLConf,
    sparkSession: SparkSession)
  extends SparkSqlParser {

  val parser = new MaximusSqlParser

  // var isRegistered = false

  override def parsePlan(sqlText: String): LogicalPlan = {
    SparkSession.setActiveSession(sparkSession)

    // UDF doesn't work for windowing function which has to be built-in function
    /*
    if (!isRegistered) { // avoid stackoverflow
      isRegistered = true
      sparkSession.udf.register(TumbleUDF.name, TumbleUDF.tumble _)
    }
     */

    try {
      parser.parse(sqlText)
    } catch {
      case _: Throwable =>
        super.parsePlan(sqlText)
    }
  }
}

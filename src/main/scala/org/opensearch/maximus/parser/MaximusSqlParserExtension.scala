package org.opensearch.maximus.parser

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.internal.SQLConf
import org.opensearch.maximus.function.TumbleUDF

class MaximusSqlParserExtension(
    conf: SQLConf,
    sparkSession: SparkSession)
  extends SparkSqlParser {

  val parser = new MaximusSqlParser

  var isRegistered = false

  override def parsePlan(sqlText: String): LogicalPlan = {
    SparkSession.setActiveSession(sparkSession)

    // TODO: make this permanent function by injectFunction rather than UDF
    //  which depends on spark session
    if (!isRegistered) { // avoid stackoverflow
      isRegistered = true
      sparkSession.udf.register(TumbleUDF.name, TumbleUDF.tumble _)
    }

    try {
      parser.parse(sqlText)
    } catch {
      case _: Throwable =>
        super.parsePlan(sqlText)
    }
  }
}

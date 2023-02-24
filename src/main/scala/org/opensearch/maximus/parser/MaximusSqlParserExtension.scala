package org.opensearch.maximus.parser

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.internal.SQLConf

class MaximusSqlParserExtension(
    conf: SQLConf)
  extends SparkSqlParser(conf) {

  val parser = new MaximusSqlParser

  override def parsePlan(sqlText: String): LogicalPlan = {
    try {
      parser.parse(sqlText)
    } catch {
      case e: Throwable =>
        super.parsePlan(sqlText)
    }
  }
}

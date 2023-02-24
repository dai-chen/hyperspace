package org.opensearch.maximus.parser

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.internal.SQLConf

class MaximusSqlParserExtension(
    conf: SQLConf)
  extends SparkSqlParser(conf) {

  val parser = new MaximusSqlParser

  override def parsePlan(sqlText: String): LogicalPlan = {
    parser.parse(sqlText)
  }
}

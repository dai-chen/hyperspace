package org.opensearch.maximus.parser

import org.apache.spark.SparkFunSuite

class MaximusSqlParserTest extends SparkFunSuite {

  val maximusSqlParser = new MaximusSqlParser

  test("Parse CREATE INDEX statement") {
    val plan = maximusSqlParser.parse(
      """
        | CREATE INDEX alb_logs_client_ip_index
        | ON default.alb_logs_table (client_ip)
        | AS 'bloomfilter'
        |""".stripMargin)

    assert(plan == MaximusCreateIndexCommand(
      Option("default"), "alb_logs_client_ip_index",
      "alb_logs_table", Seq("client_ip"), "bloomfilter"))
  }
}

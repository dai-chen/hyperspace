package org.opensearch.maximus.parser

import org.apache.spark.SparkFunSuite

class MaximusSqlParserTest extends SparkFunSuite {

  test("Parsing CREATE INDEX statement") {
    val plan = new MaximusSqlParser().parse(
      """
        | CREATE INDEX alb_logs_client_ip_index
        | ON alb_logs_table (client_ip)
        | AS 'bloomfilter'
        |""".stripMargin)

    assert(plan != null)
  }
}

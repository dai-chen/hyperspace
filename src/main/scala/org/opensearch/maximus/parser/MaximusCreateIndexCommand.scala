package org.opensearch.maximus.parser

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.execution.command.RunnableCommand

case class MaximusCreateIndexCommand(
    indexProviderName: String)
  extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    Seq()
  }
}

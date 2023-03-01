package org.opensearch.maximus.command

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.command.RunnableCommand

import com.microsoft.hyperspace.Hyperspace

case class MaximusRefreshIndexCommand(
    indexName: String,
    parentTable: TableIdentifier)
  extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val hyperspace = new Hyperspace(sparkSession)
    hyperspace.refreshIndex(indexName, "incremental")
    Seq.empty
  }
}
